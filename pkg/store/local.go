package store

import (
	"fmt"
	"strconv"
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
	var results []*logapi.LogEntry

	if parse.LHS == nil && parse.RHS == nil {
		var key string
		if parse.Level != "" {
			key = fmt.Sprintf(`.*|%s|.*`, parse.Level)
		} else if parse.Service != "" {
			key = fmt.Sprintf(`.*|.*|%s`, parse.Service)
		} else {
			return nil, fmt.Errorf("invalid query: no filter specified")
		}
		keys, vals, err := l.db.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil, fmt.Errorf("no logs found for the given filter: %w", err)
			}
			return nil, fmt.Errorf("failed to query logs: %w", err)
		}
		for i, k := range keys {
			tokens := strings.Split(k, "|")
			if len(tokens) < 3 {
				continue // Invalid key format
			}
			timestamp, err := strconv.Atoi(tokens[0])
			if err != nil {
				return nil, fmt.Errorf("invalid timestamp in key %s: %w", k, err)
			}
			level := tokens[1]
			service := tokens[2]
			message := vals[i]

			if (parse.Level == "" || level == parse.Level) && (parse.Service == "" || service == parse.Service) {
				results = append(results, &logapi.LogEntry{
					Timestamp: int64(timestamp),
					Level:     level,
					Service:   service,
					Message:   string(message),
				})
			}
		}
	} else if parse.Or {
		lhsResults, err := l.Query(*parse.LHS)
		if err != nil {
			return nil, fmt.Errorf("failed to query LHS: %w", err)
		}
		rhsResults, err := l.Query(*parse.RHS)
		if err != nil {
			return nil, fmt.Errorf("failed to query RHS: %w", err)
		}
		results = append(lhsResults, rhsResults...)
	} else {
		lhsResults, err := l.Query(*parse.LHS)
		if err != nil {
			return nil, fmt.Errorf("failed to query LHS: %w", err)
		}
		rhsResults, err := l.Query(*parse.RHS)
		if err != nil {
			return nil, fmt.Errorf("failed to query RHS: %w", err)
		}
		// Intersect the results
		resultsMap := make(map[string]*logapi.LogEntry)
		for _, lhs := range lhsResults {
			resultsMap[fmt.Sprintf("%d|%s|%s", lhs.Timestamp, lhs.Level, lhs.Service)] = lhs
		}
		for _, rhs := range rhsResults {
			key := fmt.Sprintf("%d|%s|%s", rhs.Timestamp, rhs.Level, rhs.Service)
			if entry, exists := resultsMap[key]; exists {
				results = append(results, entry)
			}
		}
	}

	return results, nil
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

	uniqueLabels, err := l.db.GetKey(`.*`) // Get all keys, we will filter them later
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
