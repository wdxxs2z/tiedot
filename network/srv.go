/* Server structure and command loop. */
package network

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/colpart"
	"github.com/HouzuoGuo/tiedot/dstruct"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"io/ioutil"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	INTER_RANK_CONN_RETRY   = 20  // Maximum retry for establishing inter-rank communication
	INTER_RANK_RETRY_EVERY  = 100 // Inter-rank connection retry interval in milliseconds
	SERVER_LOOP_RETRY_EVERY = 100 // Retry interval (in milliseconds) for waiting for schema updates in main server loop
)

// A task submitted to server loop
type Task struct {
	Fun        func() error
	Completion chan bool // Signaled upon completion of function execution
	Err        error     // Error returned by the function
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
	MainLoop    chan *Task                               // The main server loop for command processing
	BgLoop      chan func() error                        // Background task loop
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
		SchemaUpdateInProgress: true,
		InterRank:              make([]*Client, totalRank),
		MainLoop:               make(chan *Task, 100),
		BgLoop:                 make(chan func() error, 100)}
	// Create server socket
	os.Remove(srv.ServerSock)
	rpc.Register(srv)
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
				time.Sleep(INTER_RANK_RETRY_EVERY * time.Millisecond)
			}
		}
	}
	// Open my partition of the database
	if err = srv.reload(); err != nil {
		return
	}
	return
}

// Submit a task to the server and wait till its completion.
func (srv *Server) submit(fun func() error) error {
	task := &Task{Completion: make(chan bool, 1), Fun: fun}
	srv.MainLoop <- task
	<-task.Completion
	return task.Err
}

// Broadcast a message to all other servers, return true on success.
func (srv *Server) broadcast(call func(*Client) error, onErrResume bool) (err error) {
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

// Flush all buffers.
func (srv *Server) flush() error {
	for _, part := range srv.ColParts {
		part.Flush()
	}
	for _, htMap := range srv.Htables {
		for _, ht := range htMap {
			ht.File.Flush()
		}
	}
	return nil
}

// (Re)load my partition in the entire database.
func (srv *Server) reload() (err error) {
	srv.SchemaUpdateInProgress = true
	defer func() {
		srv.SchemaUpdateInProgress = false
		tdlog.Printf("Rank %d: Reload completed", srv.Rank)
	}()
	// Save whatever I already have, and get rid of everything
	srv.flush()
	srv.ColNumParts = make(map[string]int)
	srv.ColIndexPath = make(map[string][][]string)
	srv.ColIndexPathStr = make(map[string][]string)
	srv.ColParts = make(map[string]*colpart.Partition)
	srv.Htables = make(map[string]map[string]*dstruct.HashTable)
	// Read the DB directory
	files, err := ioutil.ReadDir(srv.DBDir)
	if err != nil {
		return
	}
	for _, f := range files {
		// Sub-directories are collections
		if f.IsDir() {
			// Read the "numchunks" file - its should contain a positive integer in the content
			var numchunksFH *os.File
			colName := f.Name()
			numchunksFH, err = os.OpenFile(path.Join(srv.DBDir, colName, NUMCHUNKS_FILENAME), os.O_CREATE|os.O_RDWR, 0600)
			defer numchunksFH.Close()
			if err != nil {
				return
			}
			numchunksContent, err := ioutil.ReadAll(numchunksFH)
			if err != nil {
				panic(err)
			}
			numchunks, err := strconv.Atoi(string(numchunksContent))
			if err != nil || numchunks < 1 {
				tdlog.Panicf("Rank %d: Cannot figure out number of chunks for collection %s, manually repair it maybe? numchunks: %d, err: %v", srv.Rank, srv.DBDir, numchunks, err)
			}
			srv.ColNumParts[colName] = numchunks
			srv.ColIndexPath[colName] = make([][]string, 0, 0)
			srv.ColIndexPathStr[colName] = make([]string, 0, 0)
			// Abort the program if total number of processes is not enough for a collection
			if srv.TotalRank < numchunks {
				panic(fmt.Sprintf("Please start at least %d processes, because collection %s has %d partitions", numchunks, colName, numchunks))
			}
			colDir := path.Join(srv.DBDir, colName)
			if srv.Rank < numchunks {
				tdlog.Printf("Rank %d: I am going to open my partition in %s", srv.Rank, f.Name())
				// Open data partition
				part, err := colpart.OpenPart(path.Join(colDir, CHUNK_DIRNAME_MAGIC+strconv.Itoa(srv.Rank)))
				if err != nil {
					return err
				}
				// Put the partition into server structure
				srv.ColParts[colName] = part
				srv.Htables[colName] = make(map[string]*dstruct.HashTable)
			}
			// Look for indexes in the collection
			walker := func(_ string, info os.FileInfo, err2 error) error {
				if err2 != nil {
					tdlog.Error(err)
					return nil
				}
				if info.IsDir() {
					switch {
					case strings.HasPrefix(info.Name(), HASHTABLE_DIRNAME_MAGIC):
						// Figure out indexed path - including the partition number
						indexPathStr := info.Name()[len(HASHTABLE_DIRNAME_MAGIC):]
						indexPath := strings.Split(indexPathStr, INDEX_PATH_SEP)
						// Put the schema into server structures
						srv.ColIndexPathStr[colName] = append(srv.ColIndexPathStr[colName], indexPathStr)
						srv.ColIndexPath[colName] = append(srv.ColIndexPath[colName], indexPath)
						if srv.Rank < numchunks {
							tdlog.Printf("Rank %d: I am going to open my partition in hashtable %s", srv.Rank, info.Name())
							ht, err := dstruct.OpenHash(path.Join(colDir, info.Name(), strconv.Itoa(srv.Rank)), indexPath)
							if err != nil {
								return err
							}
							srv.Htables[colName][indexPathStr] = ht
						}
					}
				}
				return nil
			}
			err = filepath.Walk(colDir, walker)
		}
	}
	return
}

// Shutdown other servers, then shutdown myself.
func (srv *Server) ShutdownAll(_ bool, _ *bool) error {
	srv.FlushAll(false, nil)
	srv.broadcast(func(client *Client) error {
		client.shutdownServerOneRankOnly()
		return nil
	}, true)
	tdlog.Printf("Rank %d: Shutdown upon client request", srv.Rank)
	os.Remove(srv.ServerSock)
	os.Exit(0)
	return nil
}

// Shutdown my server only - do not shutdown the others.
func (srv *Server) ShutdownMe(_ bool, _ *bool) error {
	srv.FlushAll(false, nil)
	tdlog.Printf("Rank %d: Shutdown upon client request", srv.Rank)
	os.Remove(srv.ServerSock)
	os.Exit(0)
	return nil
}

// Start task worker.
func (server *Server) Start() {
	tdlog.Printf("Rank %d: Now serving requests", server.Rank)
	go func() {
		// Background task loop
		defer os.Remove(server.ServerSock)
		for {
			task := <-server.BgLoop
			if err := task(); err != nil {
				tdlog.Errorf("(BgLoop) %v", err)
			}
		}
	}()
	// Foreground task loop
	for {
		task := <-server.MainLoop
		for server.SchemaUpdateInProgress {
			time.Sleep(SERVER_LOOP_RETRY_EVERY * time.Millisecond)
		}
		task.Err = task.Fun()
		task.Completion <- true
	}
}
