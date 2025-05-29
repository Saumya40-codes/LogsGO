package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"path/filepath"
	"strings"
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
	logs     []*logapi.LogEntry
	logDir   string
	db       *badger.DB
	store    store.Store
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
		logDir:   factory.DataDir,
		shutdown: make(chan struct{}),
	}
	badgerOpts := badger.DefaultOptions(filepath.Join(factory.DataDir, "index")).WithBypassLockGuard(factory.UnLockDataDir)
	badgerOpts.Logger = nil
	db, err := badger.Open(badgerOpts)
	if err != nil {
		log.Fatalf("Failed to open db %v", err)
	}
	server.db = db
	go server.periodicFlush(pkg.GetTimeDuration(factory.MaxTimeInMem))
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
	select {
	case <-ctx.Done():
		log.Println("Context cancelled. Shutting down...")
		s.GracefulStop()
		serv.shutdown <- struct{}{}
		time.Sleep(2 * time.Second) // looks safe
		serv.db.Close()
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

func (s *LogIngestorServer) QueryLogs(filter LogFilter) ([]*logapi.LogEntry, error) {
	var result []*logapi.LogEntry

	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			tokens := strings.Split(key, "|")

			level := tokens[1]
			service := tokens[2]
			msg := tokens[3]

			if filter.Level != "" && filter.Level != level {
				continue
			}
			if filter.Service != "" && filter.Service != service {
				continue
			}
			if filter.Keyword != "" && !strings.Contains(msg, filter.Keyword) {
				continue
			}
			// TODO: Support Range queries

			err := item.Value(func(val []byte) error {
				var entry logapi.LogEntry
				if err := json.Unmarshal(val, &entry); err == nil {
					result = append(result, &entry)
				}
				return nil
			})
			if err != nil {
				log.Printf("Value error: %v", err)
			}
		}
		return nil
	})

	return result, err
}
