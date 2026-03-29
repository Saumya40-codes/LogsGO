package store

import (
	"errors"
	"fmt"
	"log"
	"maps"
	"sync"
	"time"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"github.com/Saumya40-codes/LogsGO/pkg"
	"github.com/Saumya40-codes/LogsGO/pkg/internal"
	"github.com/Saumya40-codes/LogsGO/pkg/logsgoql"
	"github.com/Saumya40-codes/LogsGO/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// MemoryStore implements the Store interface using an in-memory map.
type MemoryStore struct {
	mu              sync.Mutex
	next            *Store        // Next store in the chain, if any
	maxTimeInMemory time.Duration // Maximum time in memory, after which logs are flushed to the next store
	lastFlushTime   int64         // Timestamp of the last flush operation
	shutdown        chan struct{} // Channel to signal shutdown of the store
	flushOnExit     bool
	skipList        *internal.SkipList
	series          map[LogKey]map[int64]CounterValue
	index           *ShardedLogIndex // shared log index
	meta            Labels           // contains all unique labels for this store, this is used to get unique label values
	metrics         *metrics.Metrics
}

func NewMemoryStore(next *Store, maxTimeInMemory string, flushOnExit bool, index *ShardedLogIndex, metrics *metrics.Metrics) *MemoryStore {
	mstore := &MemoryStore{
		mu:              sync.Mutex{},
		next:            next,
		maxTimeInMemory: pkg.GetTimeDuration(maxTimeInMemory),
		lastFlushTime:   0,
		shutdown:        make(chan struct{}),
		flushOnExit:     flushOnExit,
		series:          make(map[LogKey]map[int64]CounterValue),
		index:           index,
		meta: Labels{
			Services: make(map[string]int),
			Levels:   make(map[string]int),
		},
		metrics: metrics,
	}

	mstore.skipList = internal.NewSkipList()

	go mstore.startFlushTimer()
	return mstore
}

func (m *MemoryStore) Insert(logs []*logapi.LogEntry, _ map[LogKey]map[int64]CounterValue) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	timer := prometheus.NewTimer(m.metrics.IngestionDuration.WithLabelValues("memory"))
	defer timer.ObserveDuration()

	for _, lg := range logs {
		key := LogKey{lg.Service, lg.Message, lg.Level}
		ts := lg.Timestamp
		m.index.Inc(key)
		shardedSeries := m.index.getShard(key)
		if m.series[key] == nil {
			m.series[key] = make(map[int64]CounterValue)
		}
		newCounterVal := shardedSeries.data[key]
		m.series[key][ts] = *newCounterVal

		m.skipList.Insert(ts, internal.Value{Service: lg.Service, Level: lg.Level, Message: lg.Message})

		m.meta.Services[lg.Service]++
		m.meta.Levels[lg.Level]++
	}

	m.metrics.LogsIngested.WithLabelValues("memory").Add(float64(len(logs)))
	return nil
}

func (m *MemoryStore) Series(queryCtx logsgoql.QueryContext, plan *logsgoql.Plan) ([]logsgoql.Series, error) {
	return tieredSeries(queryCtx, plan, 0, m.next, func() ([]logsgoql.Series, error) {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.getSeries(queryCtx, plan)
	})
}

func (m *MemoryStore) SeriesRange(queryCtx logsgoql.QueryContext, plan *logsgoql.Plan, resolution int64) ([]logsgoql.Series, error) {
	return tieredSeries(queryCtx, plan, resolution, m.next, func() ([]logsgoql.Series, error) {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.getSeries(queryCtx, plan)
	})
}

func (m *MemoryStore) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var logsToBeFlushed []*logapi.LogEntry
	currT := time.Now().Unix()
	maxThreshold := currT - int64(m.maxTimeInMemory.Seconds())

	timer := prometheus.NewTimer(m.metrics.FlushDuration.WithLabelValues("memory", "local"))
	defer timer.ObserveDuration()
	// YOLO
	if maxThreshold < 0 {
		return errors.New("invalid maxTimeInMemory parameter set")
	}

	iter := m.skipList.Seek(internal.IteratorSearchOpts{
		Start: m.lastFlushTime + 1,
		End:   maxThreshold,
	})

	if m.next != nil {
		if localStore, ok := (*m.next).(*LocalStore); ok {
			for {
				node, ok := iter.Next()
				if !ok {
					break
				}
				ts := node.GetKey()
				logs := node.GetValues()

				for _, log := range logs {
					logsToBeFlushed = append(logsToBeFlushed, &logapi.LogEntry{
						Service:   log.Service,
						Level:     log.Level,
						Message:   log.Message,
						Timestamp: ts,
					})
				}
				m.skipList.Delete(ts)
			}
			if err := localStore.Insert(logsToBeFlushed, m.series); err != nil {
				return fmt.Errorf("failed to insert logs into next store: %w", err)
			}
		} else {
			log.Println("next store is not a LocalStore, cannot insert logs")
			return nil
		}
	}

	for _, log := range logsToBeFlushed {
		logKey := LogKey{Service: log.Service, Level: log.Level, Message: log.Message}
		if _, ok := m.series[logKey]; ok {
			if _, ok := m.series[logKey][log.Timestamp]; ok {
				delete(m.series[logKey], log.Timestamp)
				if len(m.series[logKey]) == 0 {
					delete(m.series, logKey)
				}
			} else {
				return fmt.Errorf("timestamp %v not found in series for logKey %v", log.Timestamp, logKey)
			}
		} else {
			return fmt.Errorf("logKey %v not found in series", logKey)
		}

		if _, ok := m.meta.Services[log.Service]; ok {
			m.meta.Services[log.Service]--
			if m.meta.Services[log.Service] <= 0 {
				delete(m.meta.Services, log.Service)
			}
		}
		if _, ok := m.meta.Levels[log.Level]; ok {
			m.meta.Levels[log.Level]--
			if m.meta.Levels[log.Level] <= 0 {
				delete(m.meta.Levels, log.Level)
			}
		}
	}

	return nil
}

func (m *MemoryStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.series = make(map[LogKey]map[int64]CounterValue)
	m.skipList = internal.NewSkipList()
	m.meta = Labels{
		Services: make(map[string]int),
		Levels:   make(map[string]int),
	}

	// Close the next store if it exists
	if m.next != nil {
		if localStore, ok := (*m.next).(*LocalStore); ok {
			if err := localStore.Close(); err != nil {
				return fmt.Errorf("failed to close next store: %w", err)
			}
		} else {
			return fmt.Errorf("next store is not a LocalStore, cannot close")
		}
	}

	return nil
}

func (m *MemoryStore) startFlushTimer() {
	for {
		time.Sleep(2 * time.Second) // Sleep for 2 seconds before checking the flush condition to prevent tight loop
		select {
		case <-time.After(m.maxTimeInMemory):
			// Start Flushing, what to flush should be decided by another function
			m.Flush()
			m.lastFlushTime = time.Now().Unix()

		case <-m.shutdown:
			if m.flushOnExit {
				m.Flush()
			}
			m.Close()
			return
		}
	}
}

// LabelValues returns the unique label values from the local store. We will have chain of stores, so this will return the unique values from all the stores in the chain.
func (m *MemoryStore) LabelValues(labels *Labels) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	labels.Services = make(map[string]int)
	labels.Levels = make(map[string]int)
	maps.Copy(labels.Services, m.meta.Services)
	maps.Copy(labels.Levels, m.meta.Levels)

	if localStore, ok := (*m.next).(*LocalStore); ok {
		err := localStore.LabelValues(labels)
		if err != nil {
			return fmt.Errorf("failed to get label values from local store: %w", err)
		}
	}

	return nil
}

func (m *MemoryStore) getSeries(queryCtx logsgoql.QueryContext, plan *logsgoql.Plan) ([]logsgoql.Series, error) {
	iterStart, iterEnd := seriesIterWindow(queryCtx)

	seriesByKey := make(map[LogKey][]logsgoql.Sample)

	it := m.skipList.Seek(internal.IteratorSearchOpts{
		Start: iterStart,
		End:   iterEnd,
	})

	for {
		node, ok := it.Next()
		if !ok {
			break
		}

		ts := node.GetKey()
		seenThisTs := make(map[LogKey]struct{})

		for _, v := range node.GetValues() {
			logKey := LogKey{Service: v.Service, Level: v.Level, Message: v.Message}
			if _, seen := seenThisTs[logKey]; seen {
				continue
			}
			seenThisTs[logKey] = struct{}{}

			matched, err := plan.Match(logsgoql.EntryLabels{
				Service: v.Service,
				Level:   v.Level,
				Message: v.Message,
			})
			if err != nil {
				return nil, err
			}
			if !matched {
				continue
			}

			logSeries, ok := m.series[logKey]
			if !ok {
				continue
			}
			entry, ok := logSeries[ts]
			if !ok {
				continue
			}

			seriesByKey[logKey] = append(seriesByKey[logKey], logsgoql.Sample{
				Timestamp: ts,
				Count:     uint64(entry.value),
			})
		}
	}

	results := make([]logsgoql.Series, 0, len(seriesByKey))
	for k, pts := range seriesByKey {
		results = append(results, logsgoql.Series{
			Service: k.Service,
			Level:   k.Level,
			Message: k.Message,
			Points:  pts,
		})
	}

	return results, nil
}
