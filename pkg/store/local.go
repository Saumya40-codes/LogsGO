package store

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"github.com/Saumya40-codes/LogsGO/pkg"
	"github.com/dgraph-io/badger/v4"
)

// LocalStore implements the Store interface using an persistance kv badgerDB store
type LocalStore struct {
	db            *pkg.DB
	mu            sync.Mutex
	maxTimeInDisk time.Duration // Maximum time in disk, after which logs are flushed to the next store
	// lastFlushTime int64 // Timestamp of the last flush operation
	next          *Store // Next store in the chain, if any
	shutdown      chan struct{}
	flushOnExit   bool
	lastFlushTime int64 // Timestamp of the last flush operation
}

func NewLocalStore(opts badger.Options, next *Store, maxTimeInDisk string, flushOnExit bool) (*LocalStore, error) {
	db, err := pkg.OpenDB(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open local store: %w", err)
	}

	lstore := &LocalStore{
		db:            db,
		mu:            sync.Mutex{},
		next:          next,
		maxTimeInDisk: pkg.GetTimeDuration(maxTimeInDisk),
		flushOnExit:   flushOnExit,
		lastFlushTime: 0,
	}

	go lstore.startFlushTimer()
	return lstore, nil
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
			return nil, fmt.Errorf("invalid query: no labels provided")
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

	if l.next != nil {
		if bucketStore, ok := (*l.next).(*BucketStore); ok {
			res, err := bucketStore.Query(parse)
			if err != nil {
				return nil, err
			}
			results = append(results, res...)
		}
	}

	return results, nil
}

func (l *LocalStore) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var logs []*logapi.LogEntry
	keys, vals, err := l.db.Get(".*")

	if err != nil {
		if err == badger.ErrKeyNotFound {
			return fmt.Errorf("no logs found for the given filter: %w", err)
		}
		return fmt.Errorf("failed to query logs: %w", err)
	}
	for i, k := range keys {
		tokens := strings.Split(k, "|")
		if len(tokens) < 3 {
			continue // Invalid key format
		}
		timestamp, err := strconv.Atoi(tokens[0])
		if err != nil {
			return fmt.Errorf("invalid timestamp in key %s: %w", k, err)
		}
		level := tokens[1]
		service := tokens[2]
		message := vals[i]

		if time.Now().Unix()-int64(timestamp) >= int64(l.maxTimeInDisk.Seconds()) {
			logs = append(logs, &logapi.LogEntry{
				Timestamp: int64(timestamp),
				Level:     level,
				Service:   service,
				Message:   string(message),
			})
		}
	}

	if l.next != nil {
		if bucketStore, ok := (*l.next).(*BucketStore); ok {
			err := bucketStore.Insert(logs)
			if err != nil {
				return err
			}
			fmt.Printf("%d logs have been flushed to disk\n", len(logs))
		}
	}

	// no err till here, we can delete logs either no need for next store by user or logs are flushed
	for _, lg := range logs {
		err := l.db.Delete(fmt.Sprintf("%d|%s|%s", lg.Timestamp, lg.Level, lg.Service))
		if err != nil {
			log.Printf("failed to delete log entry %d|%s|%s: %v", lg.Timestamp, lg.Level, lg.Service, err)
		}
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

	if l.next != nil {
		if bucketStore, ok := (*l.next).(*BucketStore); ok {
			nextLabels, err := bucketStore.LabelValues()
			if err != nil {
				return Labels{}, err
			}
			labels.Services = append(labels.Services, nextLabels.Services...)
			labels.Levels = append(labels.Levels, nextLabels.Levels...)
		}
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

func (l *LocalStore) startFlushTimer() {
	for {
		time.Sleep(2 * time.Second) // Sleep for 2 seconds before checking the flush condition to prevent tight loop
		select {
		case <-time.After(l.maxTimeInDisk):
			// Start Flushing, what to flush should be decided by another function
			l.Flush()
			l.lastFlushTime = time.Now().Unix()
		case <-l.shutdown:
			if l.flushOnExit {
				l.Flush()
			}
			l.Close()
			return
		}
	}
}

// Insert -> key -> fmt.Sprintf("%d|%s|%s", entry.Timestamp, entry.Level, entry.Service) TODO think about how to parse message efficiently
// we should have db instance, ideally during startup
// Flush each upstream store flushes to downstream after specified time interval, i.e. memory.Flush should flush to local.Flush, local.Flush should flush to remote.Flush
// Flush in itself should call Insert for each log entry, so that we can persist it to the db.
// Close close the db
