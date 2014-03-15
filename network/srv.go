/* Server structure and command loop. */
package network

import (
	"github.com/HouzuoGuo/tiedot/colpart"
	"github.com/HouzuoGuo/tiedot/dstruct"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"path"
	"strconv"
	"time"
)

const (
	INTER_RANK_CONN_RETRY = 20
	RETRY_EVERY           = 100 // milliseconds
)

// Tasks are queued on a server and executed one by one
type Task struct {
	Ret   chan interface{}           // Signal of function completion
	Input []string                   // Task function input parameter
	Fun   func([]string) interface{} // Task (function) with a return value type
}

// Server state and structures.
type Server struct {
	TempDir, DBDir  string // Working directory and DB directory
	ServerSock      string // Server socket file name
	Rank, TotalRank int    // Rank of current process; total number of processes
	// Schema information
	SchemaUpdateInProgress bool                  // Whether schema change is happening
	ColNumParts            map[string]int        // Collection name -> number of partitions
	ColIndexPathStr        map[string][]string   // Collection name -> indexed paths
	ColIndexPath           map[string][][]string // Collection name -> indexed path segments
	// My partition
	ColParts    map[string]*colpart.Partition            // Collection name -> partition
	Htables     map[string]map[string]*dstruct.HashTable // Collection name -> index name -> hash table
	Listener    net.Listener                             // This server socket
	InterRank   []*Client                                // Inter-rank communication connection
	MainLoop    chan *Task                               // Task loop
	ConnCounter int
}

// Start a new server.
func NewServer(rank, totalRank int, dbDir, tempDir string) (srv *Server, err error) {
	// It is very important for both client and server to initialize random seed
	rand.Seed(time.Now().UnixNano())
	if rank >= totalRank {
		panic("rank >= totalRank - should never happen")
	}
	// Create both database and working directories
	if err = os.MkdirAll(dbDir, 0700); err != nil {
		return
	}
	if err = os.MkdirAll(tempDir, 0700); err != nil {
		return
	}
	srv = &Server{Rank: rank, TotalRank: totalRank,
		ServerSock: path.Join(tempDir, strconv.Itoa(rank)),
		TempDir:    tempDir, DBDir: dbDir,
		InterRank:              make([]*Client, totalRank),
		SchemaUpdateInProgress: true,
		MainLoop:               make(chan *Task, 100)}
	// Create server socket
	os.Remove(srv.ServerSock)
	srv.Listener, err = net.Listen("unix", srv.ServerSock)
	if err != nil {
		return
	}
	// Start accepting incoming connections
	go func() {
		for {
			conn, err := srv.Listener.Accept()
			if err != nil {
				panic(err)
			}
			// Process commands from incoming connection
			go rpc.ServeConn(conn)
		}
	}()
	// Establish inter-rank communications (including a connection to myself)
	for i := 0; i < totalRank; i++ {
		for retry := 0; retry < INTER_RANK_CONN_RETRY; retry++ {
			if srv.InterRank[i], err = NewClient(tempDir, i); err == nil {
				break
			} else {
				time.Sleep(RETRY_EVERY * time.Millisecond)
			}
		}
	}
	// Open my partition of the database
	if err2 := srv.Reload(false, nil); err2 != nil {
		return nil, err2.(error)
	}
	tdlog.Printf("Rank %d: Initialization completed, listening on %s", rank, srv.ServerSock)
	return
}

// Start task worker
func (server *Server) Start() {
	defer os.Remove(server.ServerSock)
	for {
		task := <-server.MainLoop
		for server.SchemaUpdateInProgress {
			time.Sleep(RETRY_EVERY * time.Millisecond)
		}
		(task.Ret) <- task.Fun(task.Input)
	}
}

// Submit a task to the server and wait till its completion.
func (server *Server) Submit(task *Task) interface{} {
	server.MainLoop <- task
	return <-(task.Ret)
}

// Broadcast a message to all other servers, return true on success.
func (srv *Server) Broadcast(call func(*Client) error, onErrResume bool) (err error) {
	for i, rank := range srv.InterRank {
		if i == srv.Rank {
			continue
		}
		if err = call(rank); err != nil && !onErrResume {
			return
		}
	}
	return
}

// Shutdown server and delete domain socket file.
func (srv *Server) Shutdown(_ bool, _ *bool) error {
	srv.Broadcast(func(client *Client) error {
		client.ShutdownServer()
		return nil
	}, true)
	srv.FlushAll(false, nil)
	os.Remove(srv.ServerSock)
	tdlog.Printf("Rank %d: Shutdown upon client request", srv.Rank)
	os.Exit(0)
	return nil
}
