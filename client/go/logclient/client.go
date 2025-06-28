package logclient

import (
	"context"
	"crypto/tls"
	"errors"
	"time"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

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

// NewClientWithTLS creates a new gRPC client to the log server with TLS credentials.
// Addr parameter should contain the address at which logsGo gRPC server is running (which is :50051 by default)
// TLS credentials are used for secure communication with the server.
func NewLogClientWithTLS(ctx context.Context, addr string, tlsCfg *tls.Config) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
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
func (c *Client) UploadLog(ctx context.Context, entry *logapi.LogEntry) error {
	if entry == nil {
		return errors.New("no options provided for log upload")
	}
	if entry.Message == "" || entry.Service == "" || entry.Level == "" {
		return errors.New("log entry must contain Service, Level, and Message fields")
	}
	if entry.Timestamp == 0 {
		entry.Timestamp = time.Now().Unix()
	}

	res, err := c.client.UploadLog(ctx, entry)
	if err != nil {
		return err
	}

	if res == nil {
		return errors.New("failed to upload log: response is nil")
	}
	if !res.Success {
		return errors.New("failed to upload log")
	}

	return nil
}

// UploadLogs performs a batch upload, same as UploadLog but accepts slice of LogEntry to upload
func (c *Client) UploadLogs(ctx context.Context, entries *logapi.LogBatch) error {
	if err := ValidateLogs(entries); err != nil {
		return err
	}

	res, err := c.client.UploadLogs(ctx, entries)
	if err != nil {
		return err
	}

	if res == nil {
		return errors.New("failed to upload log: response is nil")
	}
	if !res.Success {
		return errors.New("failed to upload logs")
	}

	return nil
}

// Reusable function to validate log entries before upload
// This function checks if the log entries are valid and sets the timestamp to current value if not provided.
func ValidateLogs(entries *logapi.LogBatch) error {
	if entries == nil || len(entries.Entries) == 0 {
		return errors.New("no options provided for log upload")
	}

	for _, entry := range entries.Entries {
		if entry.Message == "" || entry.Service == "" || entry.Level == "" {
			return errors.New("log entry must contain Service, Level, and Message fields")
		}
		if entry.Timestamp == 0 {
			entry.Timestamp = time.Now().Unix()
		}
	}

	return nil
}
