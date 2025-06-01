package store

import (
	"fmt"
	"strings"
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

func (l *LocalStore) Query(parse LogFilter) ([]*logapi.LogEntry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return nil, fmt.Errorf("query not implemented for LocalStore")
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

// LabelValues returns the unique label values from the local store.
func (l *LocalStore) LabelValues() (Labels, error) {
	var labels Labels

	uniqueLabels, err := l.db.Get(`.*`) // Get all keys, we will filter them later
	if err != nil {
		return Labels{}, fmt.Errorf("failed to get unique labels: %w", err)
	}

	labels, err = parseLabels(uniqueLabels)
	if err != nil {
		return Labels{}, fmt.Errorf("failed to parse labels: %w", err)
	}
	return labels, nil
}

func parseLabels(uniqueKeys []string) (Labels, error) {
	labels := Labels{
		Services: make([]string, 0),
		Levels:   make([]string, 0),
	}

	for _, key := range uniqueKeys {
		tokens := strings.Split(key, "|")
		if len(tokens) < 3 {
			continue // Invalid key format
		}
		service := tokens[2]
		level := tokens[1]

		if !Contains(labels.Services, service) {
			labels.Services = append(labels.Services, service)
		}
		if !Contains(labels.Levels, level) {
			labels.Levels = append(labels.Levels, level)
		}
	}

	return labels, nil
}

// Insert -> key -> fmt.Sprintf("%d|%s|%s", entry.Timestamp, entry.Level, entry.Service) TODO think about how to parse message efficiently
// we should have db instance, ideally during startup
// Flush each upstream store flushes to downstream after specified time interval, i.e. memory.Flush should flush to local.Flush, local.Flush should flush to remote.Flush
// Flush in itself should call Insert for each log entry, so that we can persist it to the db.
// Close close the db
