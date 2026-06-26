package store

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"github.com/Saumya40-codes/LogsGO/pkg"
	"github.com/Saumya40-codes/LogsGO/pkg/logsgoql"
	"github.com/Saumya40-codes/LogsGO/pkg/metrics"
	"github.com/dgraph-io/badger/v4"
	"github.com/prometheus/client_golang/prometheus"
)

// LocalStore implements the Store interface using an persistance kv badgerDB store
type LocalStore struct {
	db            *pkg.DB
	mu            sync.RWMutex
	stopOnce      sync.Once
	maxTimeInDisk time.Duration // Maximum time in disk, after which logs are flushed to the next store
	// lastFlushTime int64 // Timestamp of the last flush operation
	next          *Store // Next store in the chain, if any
	shutdown      chan struct{}
	flushOnExit   bool
	lastFlushTime int64            // Timestamp of the last flush operation
	index         *ShardedLogIndex // shared log index
	dataDir       string           // Directory where the local store data is stored
	metrics       *metrics.Metrics
	// meta is an in-memory cache of meta.json so inserts don't re-read the file
	// on every batch (hot path under load).
	meta        Labels
	metaLoaded  bool
	currentLogs int64
}

type localStoreValue struct {
	Count  uint64            `json:"count"`
	Labels map[string]string `json:"labels,omitempty"`
}

func NewLocalStore(opts badger.Options, next *Store, maxTimeInDisk string, flushOnExit bool, index *ShardedLogIndex, metrics *metrics.Metrics) (*LocalStore, error) {
	db, err := pkg.OpenDB(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open local store: %w", err)
	}

	lstore := &LocalStore{
		db:            db,
		mu:            sync.RWMutex{},
		next:          next,
		shutdown:      make(chan struct{}),
		maxTimeInDisk: pkg.GetTimeDuration(maxTimeInDisk),
		flushOnExit:   flushOnExit,
		lastFlushTime: 0,
		index:         index,
		dataDir:       opts.Dir, // Store the directory where the local store data is stored
		metrics:       metrics,
		meta:          emptyLabels(),
	}

	// create a meta.json file to store all unique labels for this store
	dir, err := os.OpenFile(fmt.Sprintf("%s/meta.json", opts.Dir), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create meta.json file: %w", err)
	}
	if err := parseLabelsFromFile(dir, &lstore.meta); err != nil {
		dir.Close()
		return nil, fmt.Errorf("failed to load meta labels: %w", err)
	}
	lstore.metaLoaded = true
	dir.Close()
	lstore.metrics.CurrentLogsIngested.WithLabelValues("local").Set(0)

	go lstore.startFlushTimer()
	return lstore, nil
}

func (l *LocalStore) ensureMetaLocked() error {
	if l.metaLoaded {
		return nil
	}
	file, err := getMetaFile(l.dataDir)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := parseLabelsFromFile(file, &l.meta); err != nil {
		return err
	}
	l.metaLoaded = true
	return nil
}

func (l *LocalStore) Insert(logs []*logapi.LogEntry, series map[LogKey]map[int64]CounterValue) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	timer := prometheus.NewTimer(l.metrics.IngestionDuration.WithLabelValues("local"))
	defer timer.ObserveDuration()

	if len(logs) == 0 {
		return nil
	}

	if err := l.ensureMetaLocked(); err != nil {
		return fmt.Errorf("failed to load meta labels: %w", err)
	}

	batch := l.db.Conn.NewWriteBatch()
	defer batch.Cancel()

	// Stage meta updates; only apply after the badger batch succeeds so a failed
	// flush cannot leave the in-memory meta cache ahead of durable storage.
	type metaDelta struct {
		service string
		level   string
		labels  map[string]string
	}
	deltas := make([]metaDelta, 0, len(logs))

	for _, log := range logs {
		customLabels := normalizeCustomLabels(log.Labels)
		key := buildLocalStoreKey(log.Timestamp, log.Level, log.Service, log.Message, customLabels)
		logKey := LogKey{Service: log.Service, Level: log.Level, Message: log.Message, CustomLabels: labelsFingerprint(customLabels)}
		logSeries, ok := series[logKey]
		if !ok {
			return fmt.Errorf("logKey %v not found in series", logKey)
		}

		entry, ok := logSeries[log.Timestamp]
		if !ok {
			return fmt.Errorf("timestamp %v not found in logKey series", log.Timestamp)
		}

		valueBytes, err := marshalLocalStoreValue(localStoreValue{
			Count:  entry.value,
			Labels: customLabels,
		})
		if err != nil {
			return err
		}
		if err := batch.Set([]byte(key), valueBytes); err != nil {
			return fmt.Errorf("failed to save  log to DB: %w", err)
		}

		deltas = append(deltas, metaDelta{service: log.Service, level: log.Level, labels: customLabels})
	}

	if err := batch.Flush(); err != nil {
		return fmt.Errorf("failed to flush local insert batch: %w", err)
	}

	for _, d := range deltas {
		incMetaLabels(&l.meta, d.service, d.level, d.labels)
	}

	// Persist meta once per batch from the in-memory cache (avoids full re-read).
	if err := writeLabelsToPath(l.meta, l.dataDir); err != nil {
		return fmt.Errorf("failed to write meta labels to file: %w", err)
	}

	l.currentLogs += int64(len(logs))
	l.metrics.LogsIngested.WithLabelValues("local").Add(float64(len(logs)))
	l.metrics.CurrentLogsIngested.WithLabelValues("local").Set(float64(l.currentLogs))
	return nil
}

func (l *LocalStore) Series(queryCtx logsgoql.QueryContext, plan *logsgoql.Plan) ([]logsgoql.Series, error) {
	return tieredSeries(queryCtx, plan, 0, l.next, func() ([]logsgoql.Series, error) {
		l.mu.RLock()
		defer l.mu.RUnlock()
		return l.getSeries(queryCtx, plan)
	})
}

func (l *LocalStore) SeriesRange(queryCtx logsgoql.QueryContext, plan *logsgoql.Plan, resolution int64) ([]logsgoql.Series, error) {
	return tieredSeries(queryCtx, plan, resolution, l.next, func() ([]logsgoql.Series, error) {
		l.mu.RLock()
		defer l.mu.RUnlock()
		return l.getSeries(queryCtx, plan)
	})
}

func (l *LocalStore) getSeries(queryCtx logsgoql.QueryContext, plan *logsgoql.Plan) ([]logsgoql.Series, error) {
	iterStart, iterEnd := seriesIterWindow(queryCtx)

	seriesByKey := make(map[LogKey][]logsgoql.Sample)

	txn := l.db.Conn.NewTransaction(false)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Seek([]byte(localStoreSeekKey(iterStart))); it.Valid(); it.Next() {
		item := it.Item()
		k := string(item.Key())

		ts, level, service, message, keyLabels, ok := parseLocalStoreKey(k)
		if !ok {
			continue
		}
		if ts > iterEnd {
			break
		}

		valCopy, err := item.ValueCopy(nil)
		if err != nil {
			continue
		}
		stored, err := unmarshalLocalStoreValue(valCopy)
		if err != nil {
			continue
		}
		customLabels := stored.Labels
		if len(customLabels) == 0 {
			customLabels = keyLabels
		}

		matched, err := plan.Match(logsgoql.EntryLabels{Service: service, Level: level, Message: message, Labels: customLabels})
		if err != nil {
			return nil, err
		}
		if !matched {
			continue
		}

		logKey := LogKey{Service: service, Level: level, Message: message, CustomLabels: labelsFingerprint(customLabels)}
		seriesByKey[logKey] = append(seriesByKey[logKey], logsgoql.Sample{
			Timestamp: ts,
			Count:     stored.Count,
		})
	}

	results := make([]logsgoql.Series, 0, len(seriesByKey))
	for k, pts := range seriesByKey {
		sort.Slice(pts, func(i, j int) bool { return pts[i].Timestamp < pts[j].Timestamp })
		pts = compactSamplesByTimestamp(pts)

		results = append(results, logsgoql.Series{
			Service: k.Service,
			Level:   k.Level,
			Message: k.Message,
			Labels:  labelsFromFingerprint(k.CustomLabels),
			Points:  pts,
		})
	}

	return results, nil
}

func (l *LocalStore) Flush(cfg FlushConfig) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.db == nil || l.db.Conn == nil || l.db.Conn.IsClosed() {
		return nil
	}
	timer := prometheus.NewTimer(l.metrics.FlushDuration.WithLabelValues("local", "bucket"))
	defer timer.ObserveDuration()

	now := time.Now().Unix()
	var logs []*logapi.LogEntry
	series := make(map[LogKey]map[int64]CounterValue)

	// creating a r-only transac
	txn := l.db.Conn.NewTransaction(false)
	defer txn.Discard()

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	if err := l.ensureMetaLocked(); err != nil {
		return fmt.Errorf("failed to load meta labels: %w", err)
	}

	// create a batch via new NewWriteBatch to delete the data in batch
	batch := l.db.Conn.NewWriteBatch()
	defer batch.Cancel()

	for it.Seek([]byte(localStoreSeekKey(0))); it.Valid(); it.Next() {
		item := it.Item()
		k := string(item.Key())

		timestamp, level, service, message, keyLabels, ok := parseLocalStoreKey(k)
		if !ok {
			continue
		}

		if now-timestamp < int64(l.maxTimeInDisk.Seconds()) {
			continue
		}

		valCopy, err := item.ValueCopy(nil)
		if err != nil {
			continue
		}

		stored, err := unmarshalLocalStoreValue(valCopy)
		if err != nil {
			continue
		}
		customLabels := stored.Labels
		if len(customLabels) == 0 {
			customLabels = keyLabels
		}

		logKey := LogKey{Service: service, Level: level, Message: message, CustomLabels: labelsFingerprint(customLabels)}
		if series[logKey] == nil {
			series[logKey] = make(map[int64]CounterValue)
		}
		series[logKey][timestamp] = CounterValue{value: stored.Count}

		logs = append(logs, &logapi.LogEntry{
			Timestamp: timestamp,
			Level:     level,
			Service:   service,
			Message:   message,
			Labels:    cloneLabels(customLabels),
		})

		err = batch.Delete(item.KeyCopy(nil))
		if err != nil {
			log.Printf("failed to batch delete %s: %v", k, err)
		}

		decMetaLabels(&l.meta, service, level, customLabels)
	}

	if l.next != nil {
		if bucketStore, ok := (*l.next).(*BucketStore); ok {
			if err := bucketStore.Insert(logs, series); err != nil {
				return err
			}

			l.metrics.LogsFlushed.WithLabelValues("local", "bucket").Add(float64(len(logs)))
			l.currentLogs -= int64(len(logs))
			if l.currentLogs < 0 {
				l.currentLogs = 0
			}
			l.metrics.CurrentLogsIngested.WithLabelValues("local").Set(float64(l.currentLogs))

			fmt.Printf("%d logs flushed to bucket store\n", len(logs))
		}
	}

	if err := batch.Flush(); err != nil {
		return fmt.Errorf("failed to flush batched deletes: %w", err)
	}

	if err := writeLabelsToPath(l.meta, l.dataDir); err != nil {
		return fmt.Errorf("failed to write meta labels: %w", err)
	}

	l.db.RunGC()
	return nil
}

func (l *LocalStore) Close() error {
	l.stopOnce.Do(func() {
		close(l.shutdown)
	})

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.db != nil && l.db.Conn != nil && !l.db.Conn.IsClosed() {
		l.db.CloseDB()
	}
	return nil
}

// LabelValues returns the unique label values from the local store.
func (l *LocalStore) LabelValues(labels *Labels) error {
	l.mu.RLock()
	mergeLabels(labels, &l.meta)
	l.mu.RUnlock()

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
	ticker := time.NewTicker(l.maxTimeInDisk)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			l.Flush(
				FlushConfig{
					startTs:        0,
					endTs:          0,
					MaxLogsToFlush: 0,
				},
			)
			l.lastFlushTime = time.Now().Unix()
		case <-l.shutdown:
			if l.flushOnExit {
				l.Flush(
					FlushConfig{
						startTs:        0,
						endTs:          0,
						MaxLogsToFlush: 0,
					},
				)
			}
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
	*labels = emptyLabels()

	var metaLabels Labels
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&metaLabels); err != nil {
		// if EOF is reached, it means the file is empty or not yet initialized
		if errors.Is(err, io.EOF) {
			return nil
		}
		return fmt.Errorf("failed to decode meta labels from file: %w", err)
	}
	if metaLabels.Services != nil {
		labels.Services = metaLabels.Services
	}
	if metaLabels.Levels != nil {
		labels.Levels = metaLabels.Levels
	}
	if metaLabels.CustomLabels != nil {
		labels.CustomLabels = metaLabels.CustomLabels
	}
	return nil
}

func writeLabelsToPath(labels Labels, dir string) error {
	// perform atomic write to a temp file and then rename it to meta.json
	metaPath := fmt.Sprintf("%s/meta.json", dir)
	tempFile, err := os.CreateTemp(dir, "meta_*.json")
	if err != nil {
		return fmt.Errorf("failed to create temp file for meta labels: %w", err)
	}
	tempName := tempFile.Name()
	encoder := json.NewEncoder(tempFile)
	if err := encoder.Encode(labels); err != nil {
		tempFile.Close()
		os.Remove(tempName)
		return fmt.Errorf("failed to encode meta labels to temp file: %w", err)
	}
	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		os.Remove(tempName)
		return fmt.Errorf("failed to sync temp file: %w", err)
	}
	if err := tempFile.Close(); err != nil {
		os.Remove(tempName)
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	if err := os.Rename(tempName, metaPath); err != nil {
		os.Remove(tempName)
		return fmt.Errorf("failed to rename temp file to meta.json: %w", err)
	}
	return nil
}

func writeLabelsToFile(file *os.File, labels Labels, dir string) error {
	// legacy helper kept for compatibility; prefers path-based atomic write
	_ = file
	if err := writeLabelsToPath(labels, dir); err != nil {
		return err
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

func buildLocalStoreKey(timestamp int64, level, service, message string, labels map[string]string) string {
	encodedMessage := EncodeMessage(message)
	encodedLabels := encodeLabels(labels)
	if encodedLabels == "" {
		return fmt.Sprintf("%020d|%s|%s|%s", timestamp, level, service, encodedMessage)
	}
	return fmt.Sprintf("%020d|%s|%s|%s|%s", timestamp, level, service, encodedMessage, encodedLabels)
}

func localStoreSeekKey(timestamp int64) string {
	return fmt.Sprintf("%020d|", timestamp)
}

func parseLocalStoreKey(key string) (timestamp int64, level, service, message string, labels map[string]string, ok bool) {
	parts := strings.SplitN(key, "|", 5)
	if len(parts) != 4 && len(parts) != 5 {
		return 0, "", "", "", nil, false
	}

	ts, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, "", "", "", nil, false
	}

	msg, err := DecodeMessage(parts[3])
	if err != nil {
		return 0, "", "", "", nil, false
	}

	if len(parts) == 5 {
		labels = decodeLabels(parts[4])
	}
	return ts, parts[1], parts[2], msg, labels, true
}

func marshalLocalStoreValue(value localStoreValue) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal local store value: %w", err)
	}
	return data, nil
}

func unmarshalLocalStoreValue(data []byte) (localStoreValue, error) {
	var value localStoreValue
	if err := json.Unmarshal(data, &value); err == nil && value.Count > 0 {
		value.Labels = normalizeCustomLabels(value.Labels)
		return value, nil
	}

	countVal, err := strconv.ParseUint(string(data), 10, 64)
	if err != nil {
		return value, err
	}
	value.Count = countVal
	return value, nil
}

// Insert -> key -> fmt.Sprintf("%d|%s|%s", entry.Timestamp, entry.Level, entry.Service) TODO think about how to parse message efficiently
// we should have db instance, ideally during startup
// Flush each upstream store flushes to downstream after specified time interval, i.e. memory.Flush should flush to local.Flush, local.Flush should flush to remote.Flush
// Flush in itself should call Insert for each log entry, so that we can persist it to the db.
// Close close the db

// for graph view (a range query) for a query, a query should return a list of logs with timestamp, level, service, message and count
