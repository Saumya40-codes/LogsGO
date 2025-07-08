package logclient

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type Client struct {
	conn     *grpc.ClientConn
	amqpConn *amqp.Connection
	amqpChan *amqp.Channel
	client   logapi.LogIngestorClient
	qOpts    *QueueOpts
}

// Options for message queue
type QueueOpts struct {
	QueueName string
	Url       string
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

func NewLogClientWithQueue(ctx context.Context, addr string, qCfg *QueueOpts, insecure bool, tlsCfg *tls.Config) (*Client, error) {
	var grpcClient *Client
	var err error
	if insecure {
		grpcClient, err = NewLogClient(ctx, addr)
		if err != nil {
			return nil, err
		}
	} else {
		grpcClient, err = NewLogClientWithTLS(ctx, addr, tlsCfg)
		if err != nil {
			return nil, err
		}
	}

	grpcClient.qOpts = qCfg
	conn, err := amqp.Dial(qCfg.Url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	_, err = ch.QueueDeclare(
		qCfg.QueueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		conn.Close()
		ch.Close()
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	grpcClient.amqpConn = conn
	grpcClient.amqpChan = ch

	return grpcClient, nil
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

// Close closes the underlying gRPC connection and connection to message queue (if used)
func (c *Client) Close() error {
	err := c.conn.Close()
	if err != nil {
		return err
	}

	if c.amqpConn != nil {
		err = c.amqpChan.Close()
		if err != nil {
			return err
		}

		err = c.amqpConn.Close()
		if err != nil {
			return err
		}
	}

	return nil
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

func (c *Client) UploadLogsToQueue(ctx context.Context, entries *logapi.LogBatch) error {
	if err := ValidateLogs(entries); err != nil {
		return err
	}

	marshalLogs, err := proto.Marshal(entries)
	if err != nil {
		return err
	}

	err = c.amqpChan.PublishWithContext(
		ctx,
		"",
		c.qOpts.QueueName,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/octet-stream",
			Body:         []byte(marshalLogs),
		},
	)

	if err != nil {
		return err
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
