package ingestion

import (
	"context"
	"log"
	"net"
	"path/filepath"
	"sync"
	"time"

	"github.com/Saumya40-codes/LogsGO/api/auth"
	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"github.com/Saumya40-codes/LogsGO/internal/queue"
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

type QueryRequest struct {
	Query      string
	StartTs    int64
	EndTs      int64
	Resolution int64 // during range queries, this is used to determine the resolution of the query
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

	headStore := store.GetStoreChain(ctx, factory, badgerOpts)
	server.Store = headStore
	return server
}

func StartServer(ctx context.Context, serv *LogIngestorServer, addr string, authConfig auth.AuthConfig, qCfg *pkg.QueueConfig) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on port 50051: %v", err)
	}
	defer lis.Close()

	var s *grpc.Server
	var grpcOpts []grpc.ServerOption

	if authConfig.PublicKey != nil && authConfig.PublicKeyPath != "" {
		grpcOpts = append(grpcOpts, grpc.UnaryInterceptor(auth.JwtInterceptor(authConfig.PublicKey)))
	}

	if authConfig.TLSCfg != nil && authConfig.TLSCfg.LogClient.Config != nil && authConfig.TLSCfg.LogClient.Config.Enabled {
		if authConfig.TLSCfg.LogClient.Config.CertFile == "" || authConfig.TLSCfg.LogClient.Config.KeyFile == "" {
			log.Fatalf("log client tls config should have certfile and keyfile location")
		}
		creds, err := auth.GetTLSCredentials(authConfig.TLSCfg.LogClient.Config)
		if err != nil {
			log.Fatalf("failed to get TLS credentials: %v", err)
		}

		grpcOpts = append(grpcOpts, creds)
	}

	s = grpc.NewServer(grpcOpts...)

	logapi.RegisterLogIngestorServer(s, serv)

	// Run server in goroutine
	go func() {
		log.Printf("gRPC server listening at %v", lis.Addr())
		if err := s.Serve(lis); err != nil {
			log.Printf("failed to serve: %v", err)
		}
	}()

	var wg sync.WaitGroup

	if qCfg != nil {
		cfg := qCfg.Queue
		if cfg.QueueName == "" || cfg.QueueURL == "" {
			log.Fatal("provided queue config doesn't contain all necessary fields: name or url is missing")
		}
		if cfg.QueueWorkers == 0 {
			cfg.QueueWorkers = 1 // default is 1
		}

		wg.Add(int(cfg.QueueWorkers))
		for i := 0; i < int(cfg.QueueWorkers); i++ {
			go func(workerID int) {
				defer wg.Done()
				queue.StartWorker(ctx, *qCfg, serv.Store, workerID)
			}(i + 1)
		}
	}

	<-ctx.Done()
	log.Println("Context cancelled. Shutting down...")
	s.GracefulStop()
	time.Sleep(2 * time.Second) // looks safe
	serv.Store.Close()
	log.Println("Server exited")
	wg.Wait()
	if qCfg != nil {
		log.Println("Queue workers closed...")
	}
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

func (s *LogIngestorServer) MakeQuery(req QueryRequest) ([]store.QueryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cfg.QueryTime = time.Now().Unix()

	parse, err := logsgoql.ParseQuery(req.Query)
	if err != nil {
		log.Printf("failed to parse query: %v", err)
		return nil, err
	}

	var logs []store.QueryResponse

	if req.StartTs == req.EndTs {
		// instant i.e. query for a specific time
		if req.StartTs != 0 {
			s.cfg.QueryTime = req.StartTs
		} else {
			s.cfg.QueryTime = time.Now().Unix()
		}

		instantConfig := logsgoql.NewInstantQueryConfig(s.cfg.QueryTime, s.cfg.LookbackPeriod, parse)
		instantLogs, err := s.Store.QueryInstant(instantConfig)
		if err != nil {
			log.Printf("failed to query logs: %v", err)
			return nil, err
		}

		logs = changeToQueryResponse(instantLogs)
	} else {
		rangeConfig := logsgoql.NewRangeQueryConfig(req.StartTs, req.EndTs, s.cfg.LookbackPeriod, req.Resolution, parse)
		logs, err = s.Store.QueryRange(rangeConfig)
		if err != nil {
			log.Printf("failed to query logs: %v", err)
			return nil, err
		}
	}

	return logs, nil
}

func changeToQueryResponse(logs []store.InstantQueryResponse) []store.QueryResponse {
	queryResp := make([]store.QueryResponse, len(logs))
	for i, log := range logs {
		queryResp[i] = store.QueryResponse{
			Level:   log.Level,
			Service: log.Service,
			Message: log.Message,
			Series: []store.Series{
				{
					Timestamp: log.TimeStamp,
					Count:     log.Count,
				},
			},
		}
	}

	return queryResp
}
