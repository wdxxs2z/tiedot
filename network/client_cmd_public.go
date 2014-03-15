/* tiedot command implementations - client side, public APIs. */
package network

import "encoding/json"

// Reload all server configuration.
func (tc *Client) Reload() error {
	return tc.Rpc.Call("Server.Reload", false, &ignore)
}

// Tell server to shutdown, and shutdown myself (client) as well.
func (tc *Client) ShutdownServer() {
	tc.Rpc.Call("Server.Shutdown", false, &ignore)
	tc.ShutdownClient()
}

// Create a collection.
func (tc *Client) ColCreate(name string, numParts int) error {
	return tc.Rpc.Call("Server.ColCreate", ColCreateParams{name, numParts}, &ignore)
}

// Get all collection information (collection name VS number of partitions).
func (tc *Client) ColAll() (all map[string]int, err error) {
	all = make(map[string]int)
	err = tc.Rpc.Call("Server.ColAll", false, &all)
	return
}

// Rename a collection.
func (tc *Client) ColRename(oldName, newName string) error {
	return tc.Rpc.Call("Server.ColRename", ColRenameParams{oldName, newName}, &ignore)
}

// Drop a collection.
func (tc *Client) ColDrop(colName string) error {
	return tc.Rpc.Call("Server.ColDrop", colName, &ignore)
}

// Create an index.
func (tc *Client) IdxCreate(colName, idxPath string) error {
	return tc.Rpc.Call("Server.IdxCreate", IdxCreateParams{colName, idxPath}, &ignore)
}

// Get all indexed paths.
func (tc *Client) IdxAll(colName string) (paths []string, err error) {
	err = tc.Rpc.Call("Server.IdxAll", colName, &paths)
	return
}

// Drop an index.
func (tc *Client) IdxDrop(colName, idxPath string) error {
	return tc.Rpc.Call("Server.IdxDrop", IdxDropParams{colName, idxPath}, &ignore)
}

// Insert a document, return its ID.
func (tc *Client) ColInsert(colName string, js map[string]interface{}) (id uint64, err error) {
	if serialized, err := json.Marshal(js); err != nil {
		return 0, err
	} else {
		err = tc.Rpc.Call("Server.ColInsert", ColInsertParams{colName, string(serialized)}, &id)
	}
	return
}

// Get a document by ID.
func (tc *Client) ColGet(colName string, id uint64) (doc interface{}, err error) {
	var js string
	if err = tc.Rpc.Call("Server.ColGet", ColGetParams{colName, id}, &js); err != nil {
		return
	}
	err = json.Unmarshal([]byte(js), &doc)
	return
}

// Update a document by ID.
func (tc *Client) ColUpdate(colName string, id uint64, js map[string]interface{}) (err error) {
	if serialized, err := json.Marshal(js); err != nil {
		return err
	} else {
		return tc.Rpc.Call("Server.ColUpdate", ColUpdateParams{colName, string(serialized), id}, &ignore)
	}
}

// Delete a document by ID.
func (tc *Client) ColDelete(colName string, id uint64) (err error) {
	return tc.Rpc.Call("Server.ColDelete", ColDeleteParams{colName, id}, &ignore)
}

// Ping server - test client-server connection.
func (tc *Client) Ping() (err error) {
	return tc.Rpc.Call("Server.Ping", false, &ignore)
}
