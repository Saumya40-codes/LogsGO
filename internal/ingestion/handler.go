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
	"github.com/Saumya40-codes/LogsGO/pkg/logsgoql"
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

func NewLogIngestorServer(ctx context.Context, factory *pkg.IngestionFactory) *LogIngestorServer {
	server := &LogIngestorServer{
		shutdown: make(chan struct{}),
	}
	badgerOpts := badger.DefaultOptions(filepath.Join(factory.DataDir, "index")).WithBypassLockGuard(factory.UnLockDataDir)
	badgerOpts.Logger = nil

	// s3 store, if configured
	var bucketStore *store.BucketStore
	var err error
	if factory.StoreConfigPath != "" {
		bucketStore, err = store.NewBucketStore(ctx, factory.StoreConfigPath, "")
	} else if factory.StoreConfig != "" {
		bucketStore, err = store.NewBucketStore(ctx, "", factory.StoreConfig)
	}
	if err != nil {
		log.Fatalf("failed to create bucket store from give configuration %v", err)
	}

	var nextStoreS3 store.Store = bucketStore

	// disk store
	localStore, err := store.NewLocalStore(badgerOpts, &nextStoreS3, factory.MaxRetentionTime, factory.FlushOnExit)
	if err != nil {
		log.Fatalf("failed to create local store: %v", err) // We can fatal out as creating the newlogingestoreserver is one of the first things we do
	}
	var nextStore store.Store = localStore

	// memory store
	memStore := store.NewMemoryStore(&nextStore, factory.MaxTimeInMem, factory.FlushOnExit) // internally creates a goroutine to flush logs periodically
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

func (s *LogIngestorServer) MakeQuery(query string) ([]*logapi.LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	parse, err := logsgoql.ParseQuery(query)
	if err != nil {
		log.Printf("failed to parse query: %v", err)
		return nil, err
	}

	logs, err := s.Store.Query(parse)
	if err != nil {
		log.Printf("failed to query logs: %v", err)
		return nil, err
	}

	return logs, nil
}
