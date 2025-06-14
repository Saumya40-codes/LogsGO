package logclient

import (
	"context"
	"log"
	"time"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Opts struct {
	Service   string
	Level     string
	Message   string
	TimeStamp int64
}

type Client struct {
	conn   *grpc.ClientConn
	client logapi.LogIngestorClient
}

// NewClient creates a new gRPC client to the log server.
// Addr paramter should contain the address at which logsGo gRPC server is running (which is :50051 by default)
func NewLogClient(ctx context.Context, addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
	if opts == nil {
		log.Println("No options provided for log upload")
		return false
	}
	if opts.Message == "" || opts.Service == "" || opts.Level == "" {
		return false
	}
	if opts.TimeStamp == 0 {
		opts.TimeStamp = time.Now().Unix()
	}

	res, err := c.client.UploadLog(context.Background(), &logapi.LogEntry{
		Service:   opts.Service,
		Level:     opts.Level,
		Message:   opts.Message,
		Timestamp: opts.TimeStamp,
	})
	if err != nil {
		log.Printf("Uploading of log failed: %v", err)
		return false
	}
	return res.Success
}
