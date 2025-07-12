package store

import (
	"context"
	"errors"
	"log"
	"sort"
	"strings"
	"time"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"github.com/Saumya40-codes/LogsGO/pkg"
	"github.com/Saumya40-codes/LogsGO/pkg/logsgoql"
	"github.com/dgraph-io/badger/v4"
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

func GetStoreChain(ctx context.Context, factory *pkg.IngestionFactory, badgerOpts badger.Options) Store {
	shardIndex := NewShardedLogIndex()

	// s3 store, if configured
	var bucketStore *BucketStore
	var err error
	if factory.StoreConfigPath != "" {
		bucketStore, err = NewBucketStore(ctx, factory.StoreConfigPath, "", shardIndex)
	} else if factory.StoreConfig != "" {
		bucketStore, err = NewBucketStore(ctx, "", factory.StoreConfig, shardIndex)
	}
	if err != nil {
		log.Fatalf("failed to create bucket store from give configuration %v", err)
	}

	var nextStoreS3 Store
	if bucketStore != nil {
		nextStoreS3 = bucketStore
	}

	var localStore *LocalStore
	if nextStoreS3 != nil {
		localStore, err = NewLocalStore(badgerOpts, &nextStoreS3, factory.MaxRetentionTime, factory.FlushOnExit, shardIndex)
	} else {
		localStore, err = NewLocalStore(badgerOpts, nil, factory.MaxRetentionTime, factory.FlushOnExit, shardIndex)
	}
	if err != nil {
		log.Fatalf("failed to create local store: %v", err)
	}
	var nextStore Store = localStore

	// memory store
	memStore := NewMemoryStore(&nextStore, factory.MaxTimeInMem, factory.FlushOnExit, shardIndex) // internally creates a goroutine to flush logs periodically
	var headStore Store = memStore

	return headStore
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

// our logs are sorted during query and flushing time, we can use this fact to not to query all logs and use binary search
func getStartingTimeIndex(logs []*logapi.LogEntry, Ts int64) int {
	return sort.Search(len(logs), func(i int) bool {
		return logs[i].Timestamp <= Ts
	})
}
