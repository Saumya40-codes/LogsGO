package store

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
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
	lastFlushTime int64            // Timestamp of the last flush operation
	index         *ShardedLogIndex // shared log index
	dataDir       string           // Directory where the local store data is stored
}

func NewLocalStore(opts badger.Options, next *Store, maxTimeInDisk string, flushOnExit bool, index *ShardedLogIndex) (*LocalStore, error) {
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
		index:         index,
		dataDir:       opts.Dir, // Store the directory where the local store data is stored
	}

	// creata a meta.json file to store all unique labels for this store
	dir, err := os.Create(fmt.Sprintf("%s/meta.json", opts.Dir))
	if err != nil {
		return nil, fmt.Errorf("failed to create meta.json file: %w", err)
	}
	defer dir.Close()
	// Initialize the meta labels

	go lstore.startFlushTimer()
	return lstore, nil
}

func (l *LocalStore) Insert(logs []*logapi.LogEntry, series map[LogKey]map[int64]CounterValue) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// open meta.json file to store all unique labels for this store
	if len(logs) == 0 {
		return nil
	}

	file, err := getMetaFile(l.dataDir)
	if err != nil {
		return fmt.Errorf("failed to open required files in data dir: %w", err)
	}
	defer file.Close()

	var metaLabels Labels

	if err := parseLabelsFromFile(file, &metaLabels); err != nil {
		return fmt.Errorf("failed to parse meta labels: %w", err)
	}

	for _, log := range logs {
		// we store this way for efficient lookups, for querying we should convert back to series object
		key := fmt.Sprintf("%s|%s|%s|%d", log.Level, log.Service, EncodeMessage(log.Message), log.Timestamp)
		logKey := LogKey{Service: log.Service, Level: log.Level, Message: log.Message}
		logSeries, ok := series[logKey]
		if !ok {
			return fmt.Errorf("logKey %v not found in series", logKey)
		}

		entry, ok := logSeries[log.Timestamp]
		if !ok {
			return fmt.Errorf("timestamp %v not found in logKey series", log.Timestamp)
		}

		valueStr := strconv.FormatUint(entry.value, 10)
		if err := l.db.Save(key, []byte(valueStr)); err != nil {
			return fmt.Errorf("failed to save  log to DB: %w", err)
		}

		metaLabels.Services[log.Service]++
		metaLabels.Levels[log.Level]++
	}

	// Write the updated meta labels to the file
	if err := writeLabelsToFile(file, metaLabels, l.dataDir); err != nil {
		return fmt.Errorf("failed to write meta labels to file: %w", err)
	}
	return nil
}

func (l *LocalStore) QueryInstant(parse LogFilter, lookback int64, qTime int64) ([]InstantQueryResponse, error) {
	var results []InstantQueryResponse

	if parse.LHS == nil && parse.RHS == nil {
		prefix := ""
		if parse.Level != "" && parse.Service != "" {
			prefix = fmt.Sprintf("%s|%s", parse.Level, parse.Service)
		} else if parse.Level != "" {
			prefix = fmt.Sprintf("%s|", parse.Level)
		} else if parse.Service != "" {
			prefix = fmt.Sprintf(".*|%s", parse.Service)
		}

		keys, vals, err := l.db.PrefixScan(prefix)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil, fmt.Errorf("no logs found for the given filter: %w", err)
			}
			return nil, fmt.Errorf("failed to query logs: %w", err)
		}
		nearestT := -1
		for i, k := range keys {
			tokens := strings.Split(k, "|")
			if len(tokens) < 4 {
				continue // Invalid key format
			}
			timestamp, err := strconv.Atoi(tokens[3])
			if err != nil {
				return nil, fmt.Errorf("invalid timestamp in key %s: %w", k, err)
			}

			// skip this sample if it falls outside time range
			timeDiff := qTime - lookback
			if timeDiff > int64(timestamp) {
				continue
			}
			level := tokens[0]
			service := tokens[1]
			message, err := DecodeMessage(tokens[2])
			if err != nil {
				return nil, err
			}
			counterVal, err := strconv.Atoi(vals[i])
			if err != nil {
				return nil, fmt.Errorf("invalid count in value %s: %w", vals[i], err)
			}

			if ((parse.Level != "" && level == parse.Level) || (parse.Service != "" && service == parse.Service)) && (timestamp > nearestT) {
				nearestT = timestamp
				results = append(results, InstantQueryResponse{
					TimeStamp: int64(timestamp),
					Level:     level,
					Service:   service,
					Message:   message,
					Count:     uint64(counterVal),
				})
			}
		}
	} else if parse.Or {
		lhsResults, err := l.QueryInstant(*parse.LHS, lookback, qTime)
		if err != nil {
			return nil, fmt.Errorf("failed to query LHS: %w", err)
		}
		rhsResults, err := l.QueryInstant(*parse.RHS, lookback, qTime)
		if err != nil {
			return nil, fmt.Errorf("failed to query RHS: %w", err)
		}
		results = append(lhsResults, rhsResults...)
	} else {
		lhsResults, err := l.QueryInstant(*parse.LHS, lookback, qTime)
		if err != nil {
			return nil, fmt.Errorf("failed to query LHS: %w", err)
		}
		rhsResults, err := l.QueryInstant(*parse.RHS, lookback, qTime)
		if err != nil {
			return nil, fmt.Errorf("failed to query RHS: %w", err)
		}
		// Intersect the results
		for _, lhsLog := range lhsResults {
			for _, rhsLog := range rhsResults {
				if lhsLog.Service == rhsLog.Service && lhsLog.Level == rhsLog.Level && lhsLog.TimeStamp == rhsLog.TimeStamp {
					results = append(results, lhsLog)
				}
			}
		}
	}

	if l.next != nil {
		if bucketStore, ok := (*l.next).(*BucketStore); ok {
			res, err := bucketStore.QueryInstant(parse, lookback, qTime)
			if err != nil {
				return nil, err
			}
			results = append(results, res...)
		}
	}

	return results, nil
}

func (l *LocalStore) QueryRange(parse LogFilter, lookback int64, qStart, qEnd, resolution int64) ([]QueryResponse, error) {
	return nil, nil
}

func (l *LocalStore) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now().Unix()
	var logs []*logapi.LogEntry
	series := make(map[LogKey]map[int64]CounterValue)

	// creating a r-only transac
	txn := l.db.Conn.NewTransaction(false)
	defer txn.Discard()

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	file, err := getMetaFile(l.dataDir)
	if err != nil {
		return fmt.Errorf("failed to open meta file: %w", err)
	}
	defer file.Close()

	var metaLabels Labels
	if err := parseLabelsFromFile(file, &metaLabels); err != nil {
		return fmt.Errorf("failed to parse meta labels: %w", err)
	}

	// create a batch via new NewWriteBatch to delete the data in batch
	batch := l.db.Conn.NewWriteBatch()
	defer batch.Cancel()

	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		k := string(item.Key())

		tokens := strings.Split(k, "|")
		if len(tokens) < 4 {
			continue
		}

		level, service := tokens[0], tokens[1]
		message, err := DecodeMessage(tokens[2])
		if err != nil {
			continue
		}
		timestamp, err := strconv.ParseInt(tokens[3], 10, 64)
		if err != nil {
			continue
		}

		if now-timestamp < int64(l.maxTimeInDisk.Seconds()) {
			continue
		}

		valCopy, err := item.ValueCopy(nil)
		if err != nil {
			continue
		}

		countVal, err := strconv.Atoi(string(valCopy))
		if err != nil {
			continue
		}

		logKey := LogKey{Service: service, Level: level, Message: message}
		if series[logKey] == nil {
			series[logKey] = make(map[int64]CounterValue)
		}
		series[logKey][timestamp] = CounterValue{value: uint64(countVal)}

		logs = append(logs, &logapi.LogEntry{
			Timestamp: timestamp,
			Level:     level,
			Service:   service,
			Message:   message,
		})

		err = batch.Delete(item.KeyCopy(nil))
		if err != nil {
			log.Printf("failed to batch delete %s: %v", k, err)
		}

		metaLabels.Services[service]--
		if metaLabels.Services[service] <= 0 {
			delete(metaLabels.Services, service)
		}
		metaLabels.Levels[level]--
		if metaLabels.Levels[level] <= 0 {
			delete(metaLabels.Levels, level)
		}
	}

	if l.next != nil {
		if bucketStore, ok := (*l.next).(*BucketStore); ok {
			if err := bucketStore.Insert(logs, series); err != nil {
				return err
			}

			fmt.Printf("%d logs flushed to bucket store\n", len(logs))
		}
	}

	if err := batch.Flush(); err != nil {
		return fmt.Errorf("failed to flush batched deletes: %w", err)
	}

	if err := writeLabelsToFile(file, metaLabels, l.dataDir); err != nil {
		return fmt.Errorf("failed to write meta labels: %w", err)
	}

	l.db.RunGC()
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
func (l *LocalStore) LabelValues(labels *Labels) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var metaLabels Labels
	file, err := getMetaFile(l.dataDir)
	if err != nil || file == nil {
		return fmt.Errorf("failed to open meta.json file: %w", os.ErrNotExist)
	}

	err = parseLabelsFromFile(file, &metaLabels)
	if err != nil {
		return fmt.Errorf("failed to parse meta labels: %w", err)
	}

	for service := range metaLabels.Services {
		if _, exists := labels.Services[service]; !exists {
			labels.Services[service] = 0
		}
	}
	for level := range metaLabels.Levels {
		if _, exists := labels.Levels[level]; !exists {
			labels.Levels[level] = 0
		}
	}

	if l.next != nil {
		if bucketStore, ok := (*l.next).(*BucketStore); ok {
			err := bucketStore.LabelValues(labels)
			if err != nil {
				return err
			}
		}
	}

	return nil
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

func EncodeMessage(msg string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(msg))
}

func DecodeMessage(encoded string) (string, error) {
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	return string(data), err
}

func getMetaFile(dir string) (*os.File, error) {
	file, err := os.OpenFile(fmt.Sprintf("%s/meta.json", dir), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open meta.json file: %w", err)
	}
	return file, nil
}

func parseLabelsFromFile(file *os.File, labels *Labels) error {
	labels.Services = make(map[string]int)
	labels.Levels = make(map[string]int)

	var metaLabels Labels
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&metaLabels); err != nil {
		// if EOF is reached, it means the file is empty or not yet initialized
		if errors.Is(err, io.EOF) {
			return nil
		}
		return fmt.Errorf("failed to decode meta labels from file: %w", err)
	}
	labels.Services = metaLabels.Services
	labels.Levels = metaLabels.Levels
	return nil
}

func writeLabelsToFile(file *os.File, labels Labels, dir string) error {
	// peform atomic write to a temp file and then rename it to the original file
	tempFile, err := os.CreateTemp(dir, "meta_*.json")
	if err != nil {
		return fmt.Errorf("failed to create temp file for meta labels: %w", err)
	}
	defer tempFile.Close()
	encoder := json.NewEncoder(tempFile)
	if err := encoder.Encode(labels); err != nil {
		return fmt.Errorf("failed to encode meta labels to temp file: %w", err)
	}
	if err := tempFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync temp file: %w", err)
	}
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	if err := os.Rename(tempFile.Name(), file.Name()); err != nil {
		return fmt.Errorf("failed to rename temp file to meta.json: %w", err)
	}

	dirHandle, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("failed to open directory for sync: %w", err)
	}
	defer dirHandle.Close()

	if err := dirHandle.Sync(); err != nil {
		return fmt.Errorf("failed to sync directory: %w", err)
	}

	return nil
}

// Insert -> key -> fmt.Sprintf("%d|%s|%s", entry.Timestamp, entry.Level, entry.Service) TODO think about how to parse message efficiently
// we should have db instance, ideally during startup
// Flush each upstream store flushes to downstream after specified time interval, i.e. memory.Flush should flush to local.Flush, local.Flush should flush to remote.Flush
// Flush in itself should call Insert for each log entry, so that we can persist it to the db.
// Close close the db

// for graph view (a range query) for a query, a query should return a list of logs with timestamp, level, service, message and count
