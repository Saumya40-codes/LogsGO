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
	cfg      GlobalConfig
}

type LogFilter struct {
	MinTimestamp time.Time
	MaxTimestamp time.Time
	Level        string
	Service      string
	Keyword      string
}

type GlobalConfig struct { // A central global config which applies irrespective of type of store
	LookbackPeriod int64
	QueryTime      int64
}

func NewLogIngestorServer(ctx context.Context, factory *pkg.IngestionFactory) *LogIngestorServer {
	server := &LogIngestorServer{
		shutdown: make(chan struct{}),
		cfg: GlobalConfig{
			LookbackPeriod: int64(pkg.GetTimeDuration(factory.LookbackPeriod).Seconds()),
		},
	}
	badgerOpts := badger.DefaultOptions(filepath.Join(factory.DataDir, "index")).WithBypassLockGuard(factory.UnLockDataDir).WithCompactL0OnClose(true).WithValueLogFileSize(16 << 20)
	badgerOpts.Logger = nil

	shardIndex := store.NewShardedLogIndex()

	// s3 store, if configured
	var bucketStore *store.BucketStore
	var err error
	if factory.StoreConfigPath != "" {
		bucketStore, err = store.NewBucketStore(ctx, factory.StoreConfigPath, "", shardIndex)
	} else if factory.StoreConfig != "" {
		bucketStore, err = store.NewBucketStore(ctx, "", factory.StoreConfig, shardIndex)
	}
	if err != nil {
		log.Fatalf("failed to create bucket store from give configuration %v", err)
	}

	var nextStoreS3 store.Store
	if bucketStore != nil {
		nextStoreS3 = bucketStore
	}

	var localStore *store.LocalStore
	if nextStoreS3 != nil {
		localStore, err = store.NewLocalStore(badgerOpts, &nextStoreS3, factory.MaxRetentionTime, factory.FlushOnExit, shardIndex)
	} else {
		localStore, err = store.NewLocalStore(badgerOpts, nil, factory.MaxRetentionTime, factory.FlushOnExit, shardIndex)
	}
	if err != nil {
		log.Fatalf("failed to create local store: %v", err)
	}
	var nextStore store.Store = localStore

	// memory store
	memStore := store.NewMemoryStore(&nextStore, factory.MaxTimeInMem, factory.FlushOnExit, shardIndex) // internally creates a goroutine to flush logs periodically
	var headStore store.Store = memStore
	server.Store = headStore
	return server
}

func StartServer(ctx context.Context, serv *LogIngestorServer, addr string) {
	lis, err := net.Listen("tcp", addr)
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
	if err := s.Store.Insert([]*logapi.LogEntry{req}, nil); err != nil {
		return &logapi.UploadResponse{Success: false}, err
	}
	s.mu.Unlock()
	return &logapi.UploadResponse{Success: true}, nil
}

func (s *LogIngestorServer) UploadLogs(ctx context.Context, req *logapi.LogBatch) (*logapi.UploadResponse, error) {
	s.mu.Lock()
	if req == nil {
		return nil, nil // Not a best way to handle this, but we will do it for now
	}
	if err := s.Store.Insert(req.Entries, nil); err != nil {
		return &logapi.UploadResponse{Success: false}, err
	}
	s.mu.Unlock()
	return &logapi.UploadResponse{Success: true}, nil
}

func (s *LogIngestorServer) MakeQuery(query string) ([]store.QueryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cfg.QueryTime = time.Now().Unix()

	parse, err := logsgoql.ParseQuery(query)
	if err != nil {
		log.Printf("failed to parse query: %v", err)
		return nil, err
	}

	logs, err := s.Store.Query(parse, s.cfg.LookbackPeriod, s.cfg.QueryTime)
	if err != nil {
		log.Printf("failed to query logs: %v", err)
		return nil, err
	}

	return logs, nil
}
