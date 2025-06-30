package store

import (
	"errors"
	"strings"
	"time"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"github.com/Saumya40-codes/LogsGO/pkg/logsgoql"
)

// Store interfaces defines the methods that any store implementation should provide.
type Store interface {
	QueryInstant(cfg *logsgoql.InstantQueryConfig) ([]InstantQueryResponse, error) // Returns logs matching the filter
	QueryRange(cfg *logsgoql.RangeQueryConfig) ([]QueryResponse, error)            // Returns logs matching the filter in a range
	Insert(logs []*logapi.LogEntry, series map[LogKey]map[int64]CounterValue) error
	Flush() error
	Close() error
	LabelValues(labels *Labels) error // Returns all the unique label values for services and levels
	// TODO: Find what else we need here
}

type Labels struct {
	Services map[string]int
	Levels   map[string]int
	// TODO: Add support for custom labels in the future
}

type Series struct {
	Timestamp int64
	Count     uint64
}

type QueryResponse struct {
	Level   string
	Service string
	Message string
	Series  []Series
}

type LogFilter struct {
	Level   string
	Service string
	Or      bool
	RHS     *LogFilter
	LHS     *LogFilter
}

type InstantQueryResponse struct {
	Level     string
	Service   string
	TimeStamp int64
	Count     uint64
	Message   string
}

// key used for mappings
type LogKey struct {
	Service string
	Message string
	Level   string
}

type CounterValue struct {
	value uint64
}

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
