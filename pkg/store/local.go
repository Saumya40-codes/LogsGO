package store

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
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
	"github.com/cockroachdb/pebble/v2"
	"github.com/prometheus/client_golang/prometheus"
)

// LocalStore implements the Store interface using a persistent pebble kv store
type LocalStore struct {
	db            *pkg.DB
	mu            sync.Mutex
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
}

type localStoreValue struct {
	Count  uint64            `json:"count"`
	Labels map[string]string `json:"labels,omitempty"`
}

func NewLocalStore(dataDir string, next *Store, maxTimeInDisk string, flushOnExit bool, index *ShardedLogIndex, metrics *metrics.Metrics) (*LocalStore, error) {
	db, err := pkg.OpenDB(dataDir, &pebble.Options{Logger: pkg.NoopLogger{}})
	if err != nil {
		return nil, fmt.Errorf("failed to open local store: %w", err)
	}

	lstore := &LocalStore{
		db:            db,
		mu:            sync.Mutex{},
		next:          next,
		shutdown:      make(chan struct{}),
		maxTimeInDisk: pkg.GetTimeDuration(maxTimeInDisk),
		flushOnExit:   flushOnExit,
		lastFlushTime: 0,
		index:         index,
		dataDir:       dataDir,
		metrics:       metrics,
	}

	// create a meta.json file to store all unique labels for this store
	dir, err := os.OpenFile(fmt.Sprintf("%s/meta.json", dataDir), os.O_RDWR|os.O_CREATE, 0644)
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

	timer := prometheus.NewTimer(l.metrics.IngestionDuration.WithLabelValues("local"))
	defer timer.ObserveDuration()

	// open meta.json file to store all unique labels for this store
	if len(logs) == 0 {
		return nil
	}

	file, err := getMetaFile(l.dataDir)
	if err != nil {
		return fmt.Errorf("failed to open required files in data dir: %w", err)
	}
	defer file.Close()

	metaLabels := emptyLabels()

	if err := parseLabelsFromFile(file, &metaLabels); err != nil {
		return fmt.Errorf("failed to parse meta labels: %w", err)
	}

	batch := l.db.Conn.NewBatch()
	defer batch.Close()

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
		if err := batch.Set(key, valueBytes, nil); err != nil {
			return fmt.Errorf("failed to save log to DB: %w", err)
		}

		incMetaLabels(&metaLabels, log.Service, log.Level, customLabels)
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit local insert batch: %w", err)
	}

	l.metrics.LogsIngested.WithLabelValues("local").Add(float64(len(logs)))
	l.metrics.CurrentLogsIngested.WithLabelValues("local").Add(float64(len(logs)))

	// Write the updated meta labels to the file
	if err := writeLabelsToFile(file, metaLabels, l.dataDir); err != nil {
		return fmt.Errorf("failed to write meta labels to file: %w", err)
	}
	return nil
}

func (l *LocalStore) Series(queryCtx logsgoql.QueryContext, plan *logsgoql.Plan) ([]logsgoql.Series, error) {
	return tieredSeries(queryCtx, plan, 0, l.next, func() ([]logsgoql.Series, error) {
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.getSeries(queryCtx, plan)
	})
}

func (l *LocalStore) SeriesRange(queryCtx logsgoql.QueryContext, plan *logsgoql.Plan, resolution int64) ([]logsgoql.Series, error) {
	return tieredSeries(queryCtx, plan, resolution, l.next, func() ([]logsgoql.Series, error) {
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.getSeries(queryCtx, plan)
	})
}

func (l *LocalStore) getSeries(queryCtx logsgoql.QueryContext, plan *logsgoql.Plan) ([]logsgoql.Series, error) {
	iterStart, iterEnd := seriesIterWindow(queryCtx)

	seriesByKey := make(map[LogKey][]logsgoql.Sample)

	it, err := l.db.Conn.NewIter(&pebble.IterOptions{
		LowerBound: localStoreSeekKey(iterStart),
		UpperBound: localStoreUpperBound(iterEnd),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer it.Close()

	for it.First(); it.Valid(); it.Next() {
		ts, level, service, message, keyLabels, ok := parseLocalStoreKey(it.Key())
		if !ok {
			continue
		}

		val, err := it.ValueAndErr()
		if err != nil {
			continue
		}
		stored, err := unmarshalLocalStoreValue(val)
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

	if l.db == nil || l.db.Conn == nil || l.db.IsClosed() {
		return nil
	}
	timer := prometheus.NewTimer(l.metrics.FlushDuration.WithLabelValues("local", "bucket"))
	defer timer.ObserveDuration()

	now := time.Now().Unix()
	cutoff := now - int64(l.maxTimeInDisk.Seconds())
	var logs []*logapi.LogEntry
	series := make(map[LogKey]map[int64]CounterValue)

	flushLower := localStoreSeekKey(0)
	flushUpper := localStoreUpperBound(cutoff)

	it, err := l.db.Conn.NewIter(&pebble.IterOptions{
		LowerBound: flushLower,
		UpperBound: flushUpper,
	})
	if err != nil {
		return fmt.Errorf("failed to create iterator: %w", err)
	}
	defer it.Close()

	file, err := getMetaFile(l.dataDir)
	if err != nil {
		return fmt.Errorf("failed to open meta file: %w", err)
	}
	defer file.Close()

	metaLabels := emptyLabels()
	if err := parseLabelsFromFile(file, &metaLabels); err != nil {
		return fmt.Errorf("failed to parse meta labels: %w", err)
	}

	visited := false
	for it.First(); it.Valid(); it.Next() {
		visited = true
		timestamp, level, service, message, keyLabels, ok := parseLocalStoreKey(it.Key())
		if !ok {
			continue
		}

		val, err := it.ValueAndErr()
		if err != nil {
			continue
		}

		stored, err := unmarshalLocalStoreValue(val)
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

		decMetaLabels(&metaLabels, service, level, customLabels)
	}

	if l.next != nil {
		if bucketStore, ok := (*l.next).(*BucketStore); ok {
			if err := bucketStore.Insert(logs, series); err != nil {
				return err
			}

			l.metrics.LogsFlushed.WithLabelValues("local", "bucket").Add(float64(len(logs)))
			l.metrics.CurrentLogsIngested.WithLabelValues("local").Add(-float64(len(logs))) // Decrease the current logs ingested count as they are flushed by len(logs) amount

			fmt.Printf("%d logs flushed to bucket store\n", len(logs))
		}
	}

	if visited {
		if err := l.db.Conn.DeleteRange(flushLower, flushUpper, pebble.Sync); err != nil {
			return fmt.Errorf("failed to delete flushed range: %w", err)
		}
	}

	if err := writeLabelsToFile(file, metaLabels, l.dataDir); err != nil {
		return fmt.Errorf("failed to write meta labels: %w", err)
	}

	return nil
}

func (l *LocalStore) Close() error {
	l.stopOnce.Do(func() {
		close(l.shutdown)
	})

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.db != nil && l.db.Conn != nil && !l.db.IsClosed() {
		return l.db.CloseDB()
	}
	return nil
}

// LabelValues returns the unique label values from the local store.
func (l *LocalStore) LabelValues(labels *Labels) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	metaLabels := emptyLabels()
	file, err := getMetaFile(l.dataDir)
	if err != nil || file == nil {
		return fmt.Errorf("failed to open meta.json file: %w", os.ErrNotExist)
	}
	defer file.Close()

	err = parseLabelsFromFile(file, &metaLabels)
	if err != nil {
		return fmt.Errorf("failed to parse meta labels: %w", err)
	}

	mergeLabels(labels, &metaLabels)

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

// keys are a fixed 8-byte big-endian timestamp followed by pipe-separated fields,
// so byte-ordered iteration is time-ordered and range bounds are cheap to build
func buildLocalStoreKey(timestamp int64, level, service, message string, labels map[string]string) []byte {
	encodedMessage := EncodeMessage(message)
	encodedLabels := encodeLabels(labels)

	rest := level + "|" + service + "|" + encodedMessage
	if encodedLabels != "" {
		rest += "|" + encodedLabels
	}

	key := make([]byte, 8, 8+len(rest))
	binary.BigEndian.PutUint64(key, uint64(timestamp))
	return append(key, rest...)
}

func localStoreSeekKey(timestamp int64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(timestamp))
	return key
}

// exclusive upper bound covering every key at the given timestamp
func localStoreUpperBound(timestamp int64) []byte {
	if timestamp == math.MaxInt64 {
		return bytes.Repeat([]byte{0xff}, 9)
	}
	return localStoreSeekKey(timestamp + 1)
}

func parseLocalStoreKey(key []byte) (timestamp int64, level, service, message string, labels map[string]string, ok bool) {
	if len(key) <= 8 {
		return 0, "", "", "", nil, false
	}

	ts := int64(binary.BigEndian.Uint64(key[:8]))
	parts := strings.SplitN(string(key[8:]), "|", 4)
	if len(parts) != 3 && len(parts) != 4 {
		return 0, "", "", "", nil, false
	}

	msg, err := DecodeMessage(parts[2])
	if err != nil {
		return 0, "", "", "", nil, false
	}

	if len(parts) == 4 {
		labels = decodeLabels(parts[3])
	}
	return ts, parts[0], parts[1], msg, labels, true
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

