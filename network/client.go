/* Client connection to a tiedot IPC server rank. */
package network

import (
	"github.com/HouzuoGuo/tiedot/tdlog"
	"math/rand"
	"net/rpc"
	"path"
	"strconv"
	"sync"
	"time"
)

// A connection to tiedot RPC server
type Client struct {
	SrvAddr, IPCSrvTmpDir string
	SrvRank               int
	Rpc                   *rpc.Client
	Mutex                 *sync.Mutex
}

// Create a connection to a tiedot IPC server.
func NewClient(ipcSrvTmpDir string, rank int) (tc *Client, err error) {
	// It is very important for both client and server to initialize random seed
	rand.Seed(time.Now().UnixNano())
	addr := path.Join(ipcSrvTmpDir, strconv.Itoa(rank))
	rpcClient, err := rpc.Dial("unix", addr)
	if err != nil {
		return
	}
	tc = &Client{SrvAddr: addr, IPCSrvTmpDir: ipcSrvTmpDir, SrvRank: rank, Rpc: rpcClient,
		Mutex: new(sync.Mutex)}
	return
}

// Close the connection, shutdown client. Remember to call this!
func (tc *Client) ShutdownClient() {
	(*tc.Rpc).Close()
	tdlog.Printf("Client has shutdown the connection to %s", tc.SrvAddr)
}
