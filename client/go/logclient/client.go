package logclient

import (
	"context"
	"log"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn   *grpc.ClientConn
	client logapi.LogIngestorClient
}

// NewClient creates a new gRPC client to the log server.
func NewLogClient(ctx context.Context) (*Client, error) {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:   conn,
		client: logapi.NewLogIngestorClient(conn),
	}, nil
}

// Close closes the underlying gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// UploadLog sends a log entry to the server.
func (c *Client) UploadLog(opts *Opts) bool {
	res, err := c.client.UploadLog(context.Background(), &logapi.LogEntry{
		Service: opts.Service,
		Level:   opts.Level,
		Message: opts.Message,
	})
	if err != nil {
		log.Printf("Uploading of log failed: %v", err)
		return false
	}
	return res.Success
}
