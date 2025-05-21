package logclient

import (
	"context"
	"log"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func UploadLogs() bool {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to gRPC server at localhost:50051: %v", err)
	}

	defer conn.Close()

	lc := logapi.NewLogIngestorClient(conn)
	res, err := lc.UploadLog(context.Background(), &logapi.LogEntry{
		Service: "ap-south1",
		Level:   "info",
		Message: "New Endpoint Discovered",
	})

	return res.Success
}
