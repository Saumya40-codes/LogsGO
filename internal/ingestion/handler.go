package ingestion

import (
	"context"
	"log"
	"net"
	"path/filepath"
	"sync"
	"time"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"github.com/Saumya40-codes/LogsGO/pkg"
	"github.com/Saumya40-codes/LogsGO/pkg/store"
	"github.com/dgraph-io/badger/v4"
	"google.golang.org/grpc"
)

type LogIngestorServer struct {
	logapi.UnimplementedLogIngestorServer

	mu       sync.Mutex
	Store    store.Store // This is more of a linked list, this store is head which points to the next store, for now head is memory store
	shutdown chan struct{}
}

type LogFilter struct {
	MinTimestamp time.Time
	MaxTimestamp time.Time
	Level        string
	Service      string
	Keyword      string
}

func NewLogIngestorServer(factory *pkg.IngestionFactory) *LogIngestorServer {
	server := &LogIngestorServer{
		shutdown: make(chan struct{}),
	}
	badgerOpts := badger.DefaultOptions(filepath.Join(factory.DataDir, "index")).WithBypassLockGuard(factory.UnLockDataDir)
	badgerOpts.Logger = nil

	// disk store
	localStore, err := store.NewLocalStore(badgerOpts, nil, factory.MaxRetentionTime)
	if err != nil {
		log.Fatalf("failed to create local store: %v", err) // We can fatal out as creating the newlogingestoreserver is one of the first things we do
	}
	var nextStore store.Store = localStore

	// memory store
	memStore := store.NewMemoryStore(&nextStore, factory.MaxTimeInMem) // internally creates a goroutine to flush logs periodically
	var headStore store.Store = memStore
	server.Store = headStore
	return server
}

func StartServer(ctx context.Context, serv *LogIngestorServer) {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen on port 50051: %v", err)
	}
	defer lis.Close()

	s := grpc.NewServer()
	logapi.RegisterLogIngestorServer(s, serv)

	// Run server in goroutine
	go func() {
		log.Printf("gRPC server listening at %v", lis.Addr())
		if err := s.Serve(lis); err != nil {
			log.Printf("failed to serve: %v", err)
		}
	}()

	// Wait for cancellation or signal
	<-ctx.Done()
	log.Println("Context cancelled. Shutting down...")
	s.GracefulStop()
	time.Sleep(2 * time.Second) // looks safe
	serv.Store.Close()
	log.Println("Server exited")
	time.Sleep(1 * time.Second)
}

func (s *LogIngestorServer) UploadLog(ctx context.Context, req *logapi.LogEntry) (*logapi.UploadResponse, error) {
	s.mu.Lock()
	if req == nil {
		return nil, nil // Not a best way to handle this, but we will do it for now
	}
	s.Store.Insert([]*logapi.LogEntry{req})
	s.mu.Unlock()
	return &logapi.UploadResponse{Success: true}, nil
}

// This is WIP, we will implement the query logic later
// func (s *LogIngestorServer) QueryLogs(filter LogFilter) ([]*logapi.LogEntry, error) {
// 	var result []*logapi.LogEntry

// 	err := s.db.View(func(txn *badger.Txn) error {
// 		it := txn.NewIterator(badger.DefaultIteratorOptions)
// 		defer it.Close()

// 		for it.Rewind(); it.Valid(); it.Next() {
// 			item := it.Item()
// 			key := string(item.Key())
// 			tokens := strings.Split(key, "|")

// 			level := tokens[1]
// 			service := tokens[2]
// 			msg := tokens[3]

// 			if filter.Level != "" && filter.Level != level {
// 				continue
// 			}
// 			if filter.Service != "" && filter.Service != service {
// 				continue
// 			}
// 			if filter.Keyword != "" && !strings.Contains(msg, filter.Keyword) {
// 				continue
// 			}
// 			// TODO: Support Range queries

// 			err := item.Value(func(val []byte) error {
// 				var entry logapi.LogEntry
// 				if err := json.Unmarshal(val, &entry); err == nil {
// 					result = append(result, &entry)
// 				}
// 				return nil
// 			})
// 			if err != nil {
// 				log.Printf("Value error: %v", err)
// 			}
// 		}
// 		return nil
// 	})

// 	return result, err
// }
