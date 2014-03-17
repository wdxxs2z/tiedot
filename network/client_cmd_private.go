/* tiedot command implementations - client side private APIs - for testing purpose only. */
package network

import (
	"encoding/json"
	"errors"
	"fmt"
)

var ignore bool

// Insert a document. (Use ColInsert as the public API).
func (tc *Client) docInsert(colName string, doc map[string]interface{}) (id uint64, err error) {
	var js []byte
	if js, err = json.Marshal(doc); err != nil {
		return 0, errors.New(fmt.Sprintf("Client cannot serialize structure %v, error: %v", doc, err))
	}
	err = tc.Rpc.Call("Server.DocInsert", DocInsertParams{colName, string(js)}, &id)
	return
}

// Get a document by ID. (Use ColGet as the public API).
func (tc *Client) docGet(colName string, id uint64) (doc interface{}, err error) {
	var jsonStr string
	if err = tc.Rpc.Call("Server.DocGet", DocGetParams{colName, id}, &jsonStr); err != nil {
		return
	}
	err = json.Unmarshal([]byte(jsonStr), &doc)
	return
}

// Update a document by ID. (Use ColUpdate as the public API).
func (tc *Client) docUpdate(colName string, id uint64, newDoc map[string]interface{}) (newID uint64, err error) {
	var js []byte
	if js, err = json.Marshal(newDoc); err != nil {
		return 0, errors.New(fmt.Sprintf("Client cannot serialize structure %v, error: %v", newDoc, err))
	}
	err = tc.Rpc.Call("Server.DocUpdate", DocUpdateParams{colName, string(js), id}, &newID)
	return
}

// Delete a document by ID. (Use ColDelete as the public API).
func (tc *Client) docDelete(colName string, id uint64) error {
	return tc.Rpc.Call("Server.DocDelete", DocDeleteParams{colName, id}, &ignore)
}

// Put a key-value pair into hash table (no corresponding public API).
func (tc *Client) htPut(colName, indexName string, key, val uint64) error {
	return tc.Rpc.Call("Server.HTPut", HTPutParams{colName, indexName, key, val}, &ignore)
}

// Put a key-value pair into hash table (no corresponding public API).
func (tc *Client) htGet(colName, indexName string, key, limit uint64) (vals []uint64, err error) {
	err = tc.Rpc.Call("Server.HTGet", HTGetParams{colName, indexName, key, limit}, &vals)
	return
}

// Put a key-value pair into hash table (no corresponding public API).
func (tc *Client) htDelete(colName, indexName string, key, val uint64) error {
	return tc.Rpc.Call("Server.HTDelete", HTDeleteParams{colName, indexName, key, val}, &ignore)
}

// Update a document by ID, without maintaining index (use ColUpdate as the public API).
func (tc *Client) colUpdateNoIdx(colName string, id uint64, js map[string]interface{}) (err error) {
	var doc []byte
	if doc, err = json.Marshal(js); err != nil {
		return
	}
	return tc.Rpc.Call("Server.ColUpdateNoIdx", ColUpdateNoIdxParams{colName, string(doc), id}, &ignore)
}

// Delete a document by ID, without maintaining index (use ColDelete as the public API).
func (tc *Client) colDeleteNoIdx(colName string, id uint64) (err error) {
	return tc.Rpc.Call("Server.ColDeleteNoIdx", ColDeleteNoIdxParams{colName, id}, &ignore)
}

// Shutdown the server rank, without affecting other ranks, and shutdown this client as well (use ShutdownServer as the public API).
func (tc *Client) shutdownServerOneRankOnly() {
	tc.Rpc.Call("Server.ShutdownMe", false, &ignore)
	tc.ShutdownClient()
}
