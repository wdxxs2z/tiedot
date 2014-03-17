/* Server command implementations. */
package network

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/colpart"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"github.com/HouzuoGuo/tiedot/uid"
	"io/ioutil"
	"os"
	"path"
	"strconv"
)

const (
	NUMCHUNKS_FILENAME      = "numchunks"
	HASHTABLE_DIRNAME_MAGIC = "ht_"    // Hash table directory name prefix
	CHUNK_DIRNAME_MAGIC     = "chunk_" // Chunk directory name prefix
	INDEX_PATH_SEP          = ","      // Separator between index path segments
)

// Reload collection configurations.
func (srv *Server) Reload(_ bool, _ *bool) (err error) {
	return srv.submit(srv.reload)
}

// Call flush on all mapped files.
func (srv *Server) FlushAll(_ bool, _ *bool) error {
	return srv.submit(srv.flush)
}

// Create a collection.
type ColCreateParams struct {
	ColName  string
	NumParts int
}

func (srv *Server) ColCreate(in ColCreateParams, _ *bool) (err error) {
	if in.NumParts > srv.TotalRank {
		return errors.New(fmt.Sprintf("(ColCreate %s) There are not enough processes running", in.ColName))
	}
	return srv.submit(func() (err error) {
		// Make new files and directories for the collection
		if err = os.MkdirAll(path.Join(srv.DBDir, in.ColName), 0700); err != nil {
			return
		}
		if err = ioutil.WriteFile(path.Join(srv.DBDir, in.ColName, NUMCHUNKS_FILENAME), []byte(strconv.Itoa(in.NumParts)), 0600); err != nil {
			return
		}
		// Reload my config
		if err = srv.reload(); err != nil {
			return
		}
		// Inform other ranks to reload their config
		if err = srv.broadcast(func(client *Client) error {
			client.Reload()
			return nil
		}, false); err != nil {
			return errors.New(fmt.Sprintf("(ColCreate %s) Failed to reload configuration: %v", in.ColName, err))
		}
		return
	})

}

// Return all collection name VS number of partitions in JSON.
func (srv *Server) ColAll(_ bool, out *map[string]int) (neverErr error) {
	*out = make(map[string]int)
	return srv.submit(func() error {
		for k, v := range srv.ColNumParts {
			(*out)[k] = v
		}
		return nil
	})
}

// Rename a collection.
type ColRenameParams struct {
	OldName, NewName string
}

func (srv *Server) ColRename(in ColRenameParams, _ *bool) (err error) {
	// Check input names
	if in.OldName == in.NewName {
		return errors.New(fmt.Sprintf("(ColRename %s %s) New name may not be the same as old name", in.OldName, in.NewName))
	}
	return srv.submit(func() (err error) {
		if _, alreadyExists := srv.ColNumParts[in.NewName]; alreadyExists {
			return errors.New(fmt.Sprintf("(ColRename %s %s) New name is already used", in.OldName, in.NewName))
		}
		if _, exists := srv.ColNumParts[in.OldName]; !exists {
			return errors.New(fmt.Sprintf("(ColRename %s %s) Old name does not exist", in.OldName, in.NewName))
		}
		// Rename collection directory
		if err = os.Rename(path.Join(srv.DBDir, in.OldName), path.Join(srv.DBDir, in.NewName)); err != nil {
			return
		}
		// Reload myself and inform other ranks to reload their config
		if err = srv.reload(); err != nil {
			return
		}
		if err = srv.broadcast(func(client *Client) error {
			client.Reload()
			return nil
		}, false); err != nil {
			return errors.New(fmt.Sprintf("(ColRename %s %s) Failed to reload configuration: %v", in.OldName, in.NewName, err))
		}
		return
	})
}

// Drop a collection.
func (srv *Server) ColDrop(colName string, _ *bool) (err error) {
	return srv.submit(func() (err error) {
		// Check input name
		if _, exists := srv.ColNumParts[colName]; !exists {
			return errors.New(fmt.Sprintf("(ColDrop %s) Collection does not exist", colName))
		}
		// Remove the collection from file system
		if err = os.RemoveAll(path.Join(srv.DBDir, colName)); err != nil {
			return
		}
		// Reload myself and inform other ranks to reload their config
		if err = srv.reload(); err != nil {
			return
		}
		if err = srv.broadcast(func(client *Client) error {
			client.Reload()
			return nil
		}, false); err != nil {
			return errors.New(fmt.Sprintf("(ColDrop %s) Failed to reload configuration: %v", colName, err))
		}
		return
	})
}

// Only for testing client-server connection.
func (srv *Server) Ping(_ bool, _ *bool) (neverErr error) {
	return srv.submit(func() (err error) {
		return nil
	})
}

// Insert a document into my partition of the collection.
type DocInsertParams struct {
	ColName, JsonDoc string
}

func (srv *Server) DocInsert(in DocInsertParams, out *uint64) (err error) {
	return srv.submit(func() (err error) {
		// Check input collection name and JSON document string
		if col, exists := srv.ColParts[in.ColName]; !exists {
			return errors.New(fmt.Sprintf("(DocInsert %s) My rank does not own a partition of the collection", in.ColName))
		} else {
			var doc map[string]interface{}
			if err = json.Unmarshal([]byte(in.JsonDoc), &doc); err != nil {
				return
			}
			// Insert the document into my partition
			if *out, err = col.Insert(doc); err != nil {
				return
			}
		}
		return
	})
}

// Get a document from my partition of the collection.
type DocGetParams struct {
	ColName string
	ID      uint64
}

func (srv *Server) DocGet(in DocGetParams, out *string) (err error) {
	// Check input collection name and ID
	if col, exists := srv.ColParts[in.ColName]; !exists {
		return errors.New(fmt.Sprintf("(DocGet %s) My rank does not own a partition of the collection", in.ColName))
	} else {
		// Read document from partition and return
		if *out, err = col.ReadStr(in.ID); err != nil {
			return
		}
	}
	return
}

// Update a document in my partition.
type DocUpdateParams struct {
	ColName, JsonDoc string
	ID               uint64
}

func (srv *Server) DocUpdate(in DocUpdateParams, out *uint64) (err error) {
	// Check input collection name, new document JSON
	if col, exists := srv.ColParts[in.ColName]; !exists {
		return errors.New(fmt.Sprintf("(DocUpdate %s) My rank does not own a partition of the collection", in.ColName))
	} else {
		var doc map[string]interface{}
		if err = json.Unmarshal([]byte(in.JsonDoc), &doc); err != nil {
			return
		}
		if *out, err = col.Update(in.ID, doc); err != nil {
			return
		}
	}
	return
}

// Update a document in my partition.
type DocDeleteParams struct {
	ColName string
	ID      uint64
}

func (srv *Server) DocDelete(in DocDeleteParams, _ *bool) (err error) {
	// Check input collection name, new document JSON
	if col, exists := srv.ColParts[in.ColName]; !exists {
		return errors.New(fmt.Sprintf("(DocDelete %s) My rank does not own a partition of the collection", in.ColName))
	} else {
		col.Delete(in.ID)
	}
	return
}

// Put a key-value pair into hash table.
type HTPutParams struct {
	ColName, HTName string
	Key, Val        uint64
}

func (srv *Server) HTPut(in HTPutParams, _ *bool) (err error) {
	if col, exists := srv.Htables[in.ColName]; !exists {
		return errors.New(fmt.Sprintf("(HTPut %s) My rank %d does not own a partition of the hash table", in.ColName, srv.Rank))
	} else {
		if ht, exists := col[in.HTName]; !exists {
			return errors.New(fmt.Sprintf("(HTPut %s) Hash table %s does not exist", in.ColName, in.HTName))
		} else {
			ht.Put(in.Key, in.Val)
		}
	}
	return
}

// Get a key's associated values.
type HTGetParams struct {
	ColName, HTName string
	Key, Limit      uint64
}

func (srv *Server) HTGet(in HTGetParams, out *[]uint64) (err error) {
	if col, exists := srv.Htables[in.ColName]; !exists {
		return errors.New(fmt.Sprintf("(HTGet %s) My rank does not own a partition of the hash table", in.ColName))
	} else {
		if ht, exists := col[in.HTName]; !exists {
			return errors.New(fmt.Sprintf("(HTGet %s) Hash table %s does not exist", in.ColName, in.HTName))
		} else {
			vals := ht.Get(in.Key, in.Limit)
			*out = make([]uint64, len(vals))
			for i, val := range vals {
				(*out)[i] = val
			}
		}
	}
	return
}

// Remove a key-value pair.
type HTDeleteParams struct {
	ColName, HTName string
	Key, Val        uint64
}

func (srv *Server) HTDelete(in HTDeleteParams, _ *bool) (err error) {
	if col, exists := srv.Htables[in.ColName]; !exists {
		return errors.New(fmt.Sprintf("(HTDelete %s) My rank does not own a partition of the hash table", in.ColName))
	} else {
		if ht, exists := col[in.HTName]; !exists {
			return errors.New(fmt.Sprintf("(HTDelete %s) Hash table %s does not exist", in.ColName, in.HTName))
		} else {
			ht.Remove(in.Key, in.Val)
		}
	}
	return
}

// Create an index.
type IdxCreateParams struct {
	ColName, IdxPath string
}

func (srv *Server) IdxCreate(in IdxCreateParams, _ *bool) (err error) {
	// Verify that the collection exists
	if _, exists := srv.ColNumParts[in.ColName]; !exists {
		return errors.New(fmt.Sprintf("(IdxCreate %s) Collection does not exist", in.ColName))
	}
	// Create hash table directory
	if err = os.MkdirAll(path.Join(srv.DBDir, in.ColName, HASHTABLE_DIRNAME_MAGIC+in.IdxPath), 0700); err != nil {
		return
	}
	// Reload my config
	if err = srv.reload(); err != nil {
		return
	}
	// Inform other ranks to reload their config
	if err = srv.broadcast(func(client *Client) error {
		client.Reload()
		return nil
	}, false); err != nil {
		return errors.New(fmt.Sprintf("(IdxCreate %s) Failed to reload configuration: %v", in.ColName, err))
	}
	return
}

// Return list of all indexes
func (srv *Server) IdxAll(colName string, out *[]string) (err error) {
	if paths, exists := srv.ColIndexPathStr[colName]; exists {
		*out = make([]string, len(paths))
		for i, path := range paths {
			(*out)[i] = path
		}
	} else {
		return errors.New(fmt.Sprintf("(IdxAll %s) Collection does not exist", colName))
	}
	return
}

// Drop an index.
type IdxDropParams struct {
	ColName, IdxPath string
}

func (srv *Server) IdxDrop(in IdxDropParams, _ *bool) (err error) {
	// Verify that the collection exists
	if _, exists := srv.ColNumParts[in.ColName]; !exists {
		return errors.New(fmt.Sprintf("(IdxDrop %s) Collection does not exist", in.ColName))
	}
	// rm -rf index_directory
	if err = os.RemoveAll(path.Join(srv.DBDir, in.ColName, HASHTABLE_DIRNAME_MAGIC+in.IdxPath)); err != nil {
		return
	}
	// Reload my config
	if err = srv.reload(); err != nil {
		return
	}
	// Inform other ranks to reload their config
	if err = srv.broadcast(func(client *Client) error {
		client.Reload()
		return nil
	}, false); err != nil {
		return errors.New(fmt.Sprintf("(IdxDrop %s) Failed to reload configuration: %v", in.ColName, err))
	}
	return nil
}

// Contact all ranks who own the collection to put the document on all indexes.
func (srv *Server) indexDoc(colName string, docID uint64, doc interface{}) (err error) {
	numParts := uint64(srv.ColNumParts[colName])
	for i, indexPath := range srv.ColIndexPath[colName] {
		for _, toBeIndexed := range colpart.GetIn(doc, indexPath) {
			if toBeIndexed != nil {
				indexPathStr := srv.ColIndexPathStr[colName][i]
				// Figure out where to put it
				hashKey := colpart.StrHash(toBeIndexed)
				partNum := int(hashKey % numParts)
				if partNum == srv.Rank {
					// It belongs to my rank
					srv.Htables[colName][indexPathStr].Put(hashKey, docID)
				} else {
					// Go inter-rank: tell other rank to do the job
					if err = srv.InterRank[partNum].htPut(colName, indexPathStr, hashKey, docID); err != nil {
						return
					}
				}
			}
		}
	}
	return nil
}

// Contact all ranks who own the collection to remove the document from all indexes.
func (srv *Server) unindexDoc(colName string, docID uint64, doc interface{}) (err error) {
	numParts := uint64(srv.ColNumParts[colName])
	for i, indexPath := range srv.ColIndexPath[colName] {
		for _, toBeIndexed := range colpart.GetIn(doc, indexPath) {
			if toBeIndexed != nil {
				indexPathStr := srv.ColIndexPathStr[colName][i]
				// Figure out where to put it
				hashKey := colpart.StrHash(toBeIndexed)
				partNum := int(hashKey % numParts)
				if partNum == srv.Rank {
					// It belongs to my rank
					srv.Htables[colName][indexPathStr].Remove(hashKey, docID)
				} else {
					// Go inter-rank: tell other rank to do the job
					if err = srv.InterRank[partNum].htDelete(colName, indexPathStr, hashKey, docID); err != nil {
						return
					}
				}
			}
		}
	}
	return nil
}

// Insert a document and maintain hash index.
type ColInsertParams struct {
	ColName, Doc string
}

func (srv *Server) ColInsert(in ColInsertParams, out *uint64) (err error) {
	// Validate parameters
	if _, exists := srv.ColNumParts[in.ColName]; !exists {
		return errors.New(fmt.Sprintf("(ColInsert %s) Collection does not exist", in.ColName))
	}
	var jsDoc map[string]interface{}
	if err = json.Unmarshal([]byte(in.Doc), &jsDoc); err != nil || jsDoc == nil {
		return errors.New(fmt.Sprintf("(ColInsert %s) Client sent malformed JSON document", in.ColName))
	}
	// Allocate an ID for the document
	docID := uid.NextUID()
	jsDoc[uid.PK_NAME] = strconv.FormatUint(docID, 10)
	// See where the document goes
	partNum := int(docID % uint64(srv.ColNumParts[in.ColName]))
	if partNum == srv.Rank {
		// Oh I have it!
		if _, err = srv.ColParts[in.ColName].Insert(jsDoc); err != nil {
			return err
		}
	} else {
		// Tell other rank to do it
		if _, err = srv.InterRank[partNum].docInsert(in.ColName, jsDoc); err != nil {
			return err
		}
	}
	if err := srv.indexDoc(in.ColName, docID, jsDoc); err != nil {
		return err
	}
	*out = docID
	return
}

// Get a document by its unique ID (Not physical ID).
type ColGetParams struct {
	ColName string
	DocID   uint64
}

func (srv *Server) ColGet(in ColGetParams, out *string) (err error) {
	if _, exists := srv.ColNumParts[in.ColName]; !exists {
		return errors.New(fmt.Sprintf("(ColGet %s) Collection does not exist", in.ColName))
	}
	partNum := int(in.DocID % uint64(srv.ColNumParts[in.ColName]))
	if partNum == srv.Rank {
		physID, err := srv.ColParts[in.ColName].GetPhysicalID(in.DocID)
		if err != nil {
			return errors.New(fmt.Sprintf("Document %d does not exist in %s", in.DocID, in.ColName))
		}
		if *out, err = srv.ColParts[in.ColName].ReadStr(physID); err != nil {
			return err
		}
	} else {
		*out, err = srv.InterRank[partNum].ColGetJS(in.ColName, in.DocID)
	}
	return
}

// Update a document in my rank, without maintaining index index.
type ColUpdateNoIdxParams struct {
	ColName, Doc string
	DocID        uint64
}

func (srv *Server) ColUpdateNoIdx(in ColUpdateNoIdxParams, _ *bool) (err error) {
	// Validate parameters
	if _, exists := srv.ColNumParts[in.ColName]; !exists {
		return errors.New(fmt.Sprintf("(ColUpdateNoIdx %s) Collection does not exist", in.ColName))
	}
	var newDoc map[string]interface{}
	if err = json.Unmarshal([]byte(in.Doc), &newDoc); err != nil {
		return errors.New(fmt.Sprintf("(ColUpdateNoIdx %s) Client sent malformed JSON document", in.ColName))
	}
	partNum := int(in.DocID % uint64(srv.ColNumParts[in.ColName]))
	if partNum != srv.Rank {
		return errors.New(fmt.Sprintf("(ColUpdateNoIdx %s) My rank does not own the document", in.ColName))
	}
	// Now my rank owns the document and go ahead to update the document
	// Make sure that client is not overwriting document ID
	newDoc[uid.PK_NAME] = strconv.FormatUint(in.DocID, 10)
	// Read back the original document
	partition := srv.ColParts[in.ColName]
	var originalPhysicalID uint64
	if originalPhysicalID, err = partition.GetPhysicalID(in.DocID); err == nil {
		// Ordinary update
		_, err = partition.Update(originalPhysicalID, newDoc)
	} else {
		// The original document cannot be found, so we do "repair" update
		_, err = partition.Insert(newDoc)
		tdlog.Printf("(ColUpdateNoIdx %s) Repair update on %d", in.ColName, in.DocID)
	}
	return
}

// Update a document and maintain hash index.
type ColUpdateParams struct {
	ColName, Doc string
	DocID        uint64
}

func (srv *Server) ColUpdate(in ColUpdateParams, _ *bool) (err error) {
	// Validate parameters
	if _, exists := srv.ColNumParts[in.ColName]; !exists {
		return errors.New(fmt.Sprintf("(ColUpdate %s) Collection does not exist", in.ColName))
	}
	var newDoc map[string]interface{}
	if err = json.Unmarshal([]byte(in.Doc), &newDoc); err != nil {
		return errors.New(fmt.Sprintf("(ColUpdate %s) Client sent malformed JSON document", in.ColName))
	}
	partNum := int(in.DocID % uint64(srv.ColNumParts[in.ColName]))
	// Make sure that client is not overwriting document ID
	newDoc[uid.PK_NAME] = strconv.FormatUint(in.DocID, 10)
	var originalDoc interface{}
	if partNum == srv.Rank {
		// Now my rank owns the document and go ahead to update the document
		// Make sure that client is not overwriting document ID
		newDoc[uid.PK_NAME] = strconv.FormatUint(in.DocID, 10)
		// Find the physical ID of the original document, and read back the document content
		partition := srv.ColParts[in.ColName]
		var originalPhysicalID uint64
		if originalPhysicalID, err = partition.GetPhysicalID(in.DocID); err == nil {
			if err = partition.Read(originalPhysicalID, &originalDoc); err != nil {
				tdlog.Printf("(ColUpdate %s) Cannot read back %d, will continue document update anyway", in.ColName, in.DocID)
			}
			// Ordinary update
			_, err = partition.Update(originalPhysicalID, newDoc)
		} else {
			// The original document cannot be found, so we do "repair" update
			tdlog.Printf("(ColUpdate %s) Repair update on %d", in.ColName, in.DocID)
			_, err = partition.Insert(newDoc)
		}
		if err != nil {
			return
		}
	} else {
		// If my rank does not own the document, coordinate this update with other ranks, and to prevent deadlock...
		// Contact other rank to get document content
		if originalDoc, err = srv.InterRank[partNum].ColGet(in.ColName, in.DocID); err != nil {
			return
		}
		// Contact other rank to update document without maintaining index
		if err = srv.InterRank[partNum].colUpdateNoIdx(in.ColName, in.DocID, newDoc); err != nil {
			return
		}
	}
	// No matter where the document is physically located at, my rank always coordinates index maintenance
	if originalDoc != nil {
		if err = srv.unindexDoc(in.ColName, in.DocID, originalDoc); err != nil {
			return
		}
	}
	return srv.indexDoc(in.ColName, in.DocID, newDoc)
}

// Delete a document by its unique ID (Not physical ID).
type ColDeleteNoIdxParams struct {
	ColName string
	DocID   uint64
}

func (srv *Server) ColDeleteNoIdx(in ColDeleteNoIdxParams, _ *bool) (err error) {
	// Validate parameters
	if _, exists := srv.ColNumParts[in.ColName]; !exists {
		return errors.New(fmt.Sprintf("(ColDeleteNoIdx %s) Collection does not exist", in.ColName))
	}
	partNum := int(in.DocID % uint64(srv.ColNumParts[in.ColName]))
	if partNum != srv.Rank {
		return errors.New(fmt.Sprintf("(ColDeleteNoIdx %s) My rank does not own the document", in.ColName))
	}
	// Now my rank owns the document and go ahead to delete the document
	partition := srv.ColParts[in.ColName]
	// Find the physical ID of the document
	var originalPhysicalID uint64
	originalPhysicalID, err = partition.GetPhysicalID(in.DocID)
	if err != nil {
		// The original document cannot be found - so it has already been deleted
		return nil
	}
	// Delete the document
	partition.Delete(originalPhysicalID)
	return
}

// Delete a document by its unique ID (Not physical ID).
type ColDeleteParams struct {
	ColName string
	DocID   uint64
}

func (srv *Server) ColDelete(in ColDeleteNoIdxParams, _ *bool) (err error) {
	// Validate parameters
	if _, exists := srv.ColNumParts[in.ColName]; !exists {
		return errors.New(fmt.Sprintf("(ColDelete %s) Collection does not exist", in.ColName))
	}
	partNum := int(in.DocID % uint64(srv.ColNumParts[in.ColName]))
	var originalDoc interface{}
	if partNum == srv.Rank {
		// Now my rank owns the document and go ahead to delete the document
		// Read back the original document
		partition := srv.ColParts[in.ColName]
		var originalPhysicalID uint64
		originalPhysicalID, err = partition.GetPhysicalID(in.DocID)
		if err == nil {
			if err = partition.Read(originalPhysicalID, &originalDoc); err != nil {
				tdlog.Printf("(ColDelete %s) Cannot read back %d, will continue to delete anyway", in.ColName, in.DocID)
			}
		} else {
			// The original document cannot be found - so it has already been deleted
			return nil
		}
		// Delete the document
		partition.Delete(originalPhysicalID)
	} else {
		// If my rank does not own the document, coordinate this update with other ranks, and to prevent deadlock...
		// Contact other rank to get document content
		if originalDoc, err = srv.InterRank[partNum].ColGet(in.ColName, in.DocID); err != nil {
			return
		}
		if err = srv.InterRank[partNum].colDeleteNoIdx(in.ColName, in.DocID); err != nil {
			return
		}
	}
	// No matter where the document is physically located at, my rank always coordinates index maintenance
	return srv.unindexDoc(in.ColName, in.DocID, originalDoc)
}
