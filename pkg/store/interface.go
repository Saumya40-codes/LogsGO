package store

import (
	"errors"
	"strings"
	"time"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
)

// Store interfaces defines the methods that any store implementation should provide.
type Store interface {
	Query(filter LogFilter) ([]*logapi.LogEntry, error) // Returns logs matching the filter
	Insert(logs []*logapi.LogEntry) error
	Flush() error
	Close() error
	LabelValues() (Labels, error) // Returns all the unique label values for services and levels
	// TODO: Find what else we need here
}

type Labels struct {
	Services []string
	Levels   []string
	// TODO: Add keywords or other labels if needed
}

type LogFilter struct {
	Level   string
	Service string
	Or      bool
	RHS     *LogFilter
	LHS     *LogFilter
}

var (
	ErrInvalidQuery   = errors.New("invalid_query")
	ErrNotFound       = errors.New("not_found")
	ErrInternal       = errors.New("internal_error")
	ErrNotImplemented = errors.New("not_implemented")
	ErrInvalidLabel   = errors.New("invalid_label")
)

func Contains(slice []string, item string) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

func validateConfiguration(config BucketStoreConfig) error {
	if strings.TrimSpace(config.Provider) == "" {
		return errors.New("provider field in configuration is empty")
	}

	if strings.TrimSpace(config.Endpoint) == "" {
		return errors.New("endpoint field in configuration is empty")
	}

	if strings.TrimSpace(config.Bucket) == "" {
		return errors.New("bucket field in configuration is empty")
	}

	if strings.TrimSpace(config.Region) == "" {
		return errors.New("region field in configuration is empty")
	}

	if strings.TrimSpace(config.AccessKey) == "" {
		return errors.New("access_key field in configuration is empty")
	}

	if strings.TrimSpace(config.SecretKey) == "" {
		return errors.New("secret_key field in configuration is empty")
	}

	return nil
}

func getNextTimeStamp(current int64, base time.Duration) int64 {
	return current + time.Unix(int64(base.Seconds()), 0).Unix()
}
