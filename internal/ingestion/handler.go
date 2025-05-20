package ingestion

import (
	"context"
	"fmt"
	"log"
	"net"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"google.golang.org/grpc"
)

type LogIngestorServer struct {
	logapi.UnimplementedLogIngestorServer
}

func StartServer() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen on port 50051: %v", err)
	}

	s := grpc.NewServer()
	logapi.RegisterLogIngestorServer(s, &LogIngestorServer{})
	log.Printf("gRPC server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func (s *LogIngestorServer) UploadLog(ctx context.Context, req *logapi.LogEntry) (*logapi.UploadResponse, error) {
	fmt.Println(req)
	log.Printf("Received log: %v", req)
	return &logapi.UploadResponse{Success: true}, nil
}
