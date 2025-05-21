package ingestion

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"github.com/Saumya40-codes/LogsGO/pkg"
	"google.golang.org/grpc"
)

type LogIngestorServer struct {
	logapi.UnimplementedLogIngestorServer

	mu       sync.Mutex
	logs     []*logapi.LogEntry
	logDir   string
	shutdown chan struct{}
}

func NewLogIngestorServer(logDir string) *LogIngestorServer {
	server := &LogIngestorServer{
		logDir:   logDir,
		shutdown: make(chan struct{}),
	}
	go server.periodicFlush(1 * time.Hour)
	return server
}

func StartServer(factory *pkg.IngestionFactory) {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen on port 50051: %v", err)
	}

	s := grpc.NewServer()
	server := NewLogIngestorServer(factory.DataDir)
	logapi.RegisterLogIngestorServer(s, server)

	log.Printf("gRPC server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
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

	timestamp := time.Now().Format("2006-01-02_15-04-05")
	filename := filepath.Join(s.logDir, "logs_"+timestamp+".json")

	if err := os.MkdirAll(s.logDir, 0755); err != nil {
		log.Printf("failed to create log directory: %v", err)
		return
	}

	file, err := os.Create(filename)
	if err != nil {
		log.Printf("failed to create log file: %v", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	for _, entry := range logsToFlush {
		if err := encoder.Encode(entry); err != nil {
			log.Printf("failed to encode log: %v", err)
		}
	}

	log.Printf("Flushed %d logs to %s", len(logsToFlush), filename)
}
