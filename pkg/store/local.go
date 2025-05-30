package store

import (
	"fmt"
	"sync"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"github.com/Saumya40-codes/LogsGO/pkg"
	"github.com/dgraph-io/badger/v4"
)

// LocalStore implements the Store interface using an persistance kv badgerDB store
type LocalStore struct {
	db            *pkg.DB
	mu            sync.Mutex
	maxTimeInDisk int64 // Maximum time in disk, after which logs are flushed to the next store
	// lastFlushTime int64 // Timestamp of the last flush operation
	next Store // Next store in the chain, if any
}

func NewLocalStore(opts badger.Options, next Store, maxTimeInDisk string) (*LocalStore, error) {
	db, err := pkg.OpenDB(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open local store: %w", err)
	}
	return &LocalStore{
		db:            db,
		mu:            sync.Mutex{},
		next:          next,
		maxTimeInDisk: int64(pkg.GetTimeDuration(maxTimeInDisk).Seconds()),
	}, nil
}

func (l *LocalStore) Insert(logs []*logapi.LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, log := range logs {
		key := fmt.Sprintf("%d|%s|%s", log.Timestamp, log.Level, log.Service)
		value := log.Message
		err := l.db.Save(key, []byte(value))
		if err != nil {
			return fmt.Errorf("failed to insert log entry: %w", err)
		}
	}

	return nil
}

func (l *LocalStore) Query() {
	// TODO: Implement query logic based on the filter
}

func (l *LocalStore) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Flush logic can be implemented here if needed
	// For now, we assume that the data is persisted immediately on Insert
	if l.next != nil {
		return l.next.Insert(nil) // This will be a remote store, to be implemented later
	}
	return nil
}

func (l *LocalStore) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.db != nil {
		l.db.CloseDB()
	}
	return nil
}

// Insert -> key -> fmt.Sprintf("%d|%s|%s", entry.Timestamp, entry.Level, entry.Service) TODO think about how to parse message efficiently
// we should have db instance, ideally during startup
// Flush each upstream store flushes to downstream after specified time interval, i.e. memory.Flush should flush to local.Flush, local.Flush should flush to remote.Flush
// Flush in itself should call Insert for each log entry, so that we can persist it to the db.
// Close close the db
