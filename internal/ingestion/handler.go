package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"path/filepath"
	"sync"
	"time"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"github.com/Saumya40-codes/LogsGO/pkg"
	"github.com/dgraph-io/badger/v4"
	"google.golang.org/grpc"
)

type LogIngestorServer struct {
	logapi.UnimplementedLogIngestorServer

	mu       sync.Mutex
	logs     []*logapi.LogEntry
	logDir   string
	db       *badger.DB
	shutdown chan struct{}
}

func NewLogIngestorServer(logDir string, maxTimeMem string) *LogIngestorServer {
	server := &LogIngestorServer{
		logDir:   logDir,
		shutdown: make(chan struct{}),
	}
	badgerOpts := badger.DefaultOptions(filepath.Join(logDir, "index"))
	badgerOpts.Logger = nil
	db, err := badger.Open(badgerOpts)
	if err != nil {
		log.Fatalf("Failed to open db %v", err)
	}
	server.db = db
	go server.periodicFlush(pkg.GetTimeDuration(maxTimeMem))
	return server
}

func StartServer(ctx context.Context, factory *pkg.IngestionFactory) {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen on port 50051: %v", err)
	}
	defer lis.Close()

	s := grpc.NewServer()
	server := NewLogIngestorServer(factory.DataDir, factory.MaxTimeInMem)
	logapi.RegisterLogIngestorServer(s, server)

	// Run server in goroutine
	go func() {
		log.Printf("gRPC server listening at %v", lis.Addr())
		if err := s.Serve(lis); err != nil {
			log.Printf("failed to serve: %v", err)
		}
	}()

	// Wait for cancellation or signal
	select {
	case <-ctx.Done():
		log.Println("Context cancelled. Shutting down...")
		s.GracefulStop()
		server.shutdown <- struct{}{}
		time.Sleep(2 * time.Second) // looks safe
		server.db.Close()
		log.Println("Server exited")
		time.Sleep(1 * time.Second)
	}
}

func (s *LogIngestorServer) UploadLog(ctx context.Context, req *logapi.LogEntry) (*logapi.UploadResponse, error) {
	s.mu.Lock()
	s.logs = append(s.logs, req)
	s.mu.Unlock()
	return &logapi.UploadResponse{Success: true}, nil
}

func (s *LogIngestorServer) periodicFlush(interval time.Duration) {
	for {
		select {
		case <-time.After(interval):
			s.flushToDisk()
		case <-s.shutdown:
			s.flushToDisk()
			return
		}
	}
}

func (s *LogIngestorServer) flushToDisk() {
	s.mu.Lock()
	logsToFlush := s.logs
	s.logs = nil
	s.mu.Unlock()

	if len(logsToFlush) == 0 {
		return
	}

	for _, entry := range logsToFlush {
		key := fmt.Sprintf("%d|%s|%s|%s", entry.Timestamp, entry.Level, entry.Service, entry.Message)

		val, _ := json.Marshal(entry)
		err := s.db.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte(key), val)
		})
		if err != nil {
			log.Printf("Failed to write to Badger: %v", err)
		}
	}

	log.Printf("Flushed %d logs to db", len(logsToFlush))
	s.logs = s.logs[:0]
}
