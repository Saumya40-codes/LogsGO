package store

import (
	"fmt"
	"log"
	"maps"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
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
	stopOnce        sync.Once
	next            *Store        // Next store in the chain, if any
	maxTimeInMemory time.Duration // Maximum time in memory, after which logs are flushed to the next store
	maxLogsInMem    int64         // Maximum number of logs in memory before flushing to the next store
	backoff         int32
	maxLogReached   chan struct{}
	shutdown        chan struct{} // Channel to signal shutdown of the store
	flushOnExit     bool
	skipList        *internal.SkipList
	series          map[LogKey]map[int64]CounterValue
	index           *ShardedLogIndex // shared log index
	meta            Labels           // contains all unique labels for this store, this is used to get unique label values
	metrics         *metrics.Metrics
	totalLogs       int64
	done            chan struct{}
}

func NewMemoryStore(next *Store, maxTimeInMemory string, maxLogsInMem int64, flushOnExit bool, index *ShardedLogIndex, metrics *metrics.Metrics) *MemoryStore {
	mstore := &MemoryStore{
		mu:              sync.Mutex{},
		next:            next,
		maxTimeInMemory: pkg.GetTimeDuration(maxTimeInMemory),
		maxLogsInMem:    maxLogsInMem,
		backoff:         getInitialBackoff(maxLogsInMem),
		maxLogReached:   make(chan struct{}, 1),
		shutdown:        make(chan struct{}),
		flushOnExit:     flushOnExit,
		series:          make(map[LogKey]map[int64]CounterValue),
		index:           index,
		meta: Labels{
			Services: make(map[string]int),
			Levels:   make(map[string]int),
		},
		metrics:   metrics,
		totalLogs: 0,
		done:      make(chan struct{}, 2),
	}

	mstore.skipList = internal.NewSkipList()

	go mstore.startFlushTimer()
	if mstore.maxLogsInMem > 0 {
		go mstore.startBackoffResetTimer()
	}
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

	m.totalLogs += int64(len(logs))

	if m.totalLogs > m.maxLogsInMem {
		select {
		case m.maxLogReached <- struct{}{}:
		default:
		}
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

func (m *MemoryStore) Flush(cfg FlushConfig) error {
	var logsToBeFlushed []*logapi.LogEntry
	seriesToFlush := make(map[LogKey]map[int64]CounterValue)
	var logsCount int64
	var localStore *LocalStore

	func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		currT := time.Now().Unix()
		maxThreshold := currT - int64(m.maxTimeInMemory.Seconds())

		timer := prometheus.NewTimer(m.metrics.FlushDuration.WithLabelValues("memory", "local"))
		defer timer.ObserveDuration()
		// YOLO
		if maxThreshold < 0 {
			return
		}

		if cfg.endTs == 0 {
			cfg.endTs = maxThreshold
		}

		if m.next != nil {
			if nextStore, ok := (*m.next).(*LocalStore); ok {
				localStore = nextStore
			} else {
				log.Println("next store is not a LocalStore, cannot insert logs")
				return
			}
		}

		iter := m.skipList.Seek(internal.IteratorSearchOpts{
			Start: cfg.startTs,
			End:   cfg.endTs,
		})

		for {
			node, ok := iter.Next()
			if !ok {
				break
			}
			ts := node.GetKey()
			logs := node.GetValues()

			logsCount += int64(len(logs))

			for _, log := range logs {
				entry := &logapi.LogEntry{
					Service:   log.Service,
					Level:     log.Level,
					Message:   log.Message,
					Timestamp: ts,
				}
				logsToBeFlushed = append(logsToBeFlushed, entry)

				logKey := LogKey{Service: log.Service, Level: log.Level, Message: log.Message}
				logSeries, ok := m.series[logKey]
				if !ok {
					return
				}
				counterVal, ok := logSeries[ts]
				if !ok {
					return
				}
				if seriesToFlush[logKey] == nil {
					seriesToFlush[logKey] = make(map[int64]CounterValue)
				}
				seriesToFlush[logKey][ts] = counterVal
			}
			if logsCount >= cfg.MaxLogsToFlush && cfg.MaxLogsToFlush > 0 {
				break
			}
		}
	}()

	if localStore != nil {
		if err := localStore.Insert(logsToBeFlushed, seriesToFlush); err != nil {
			return fmt.Errorf("failed to insert logs into next store: %w", err)
		}
	}

	func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		for logKey, samples := range seriesToFlush {
			logSeries, ok := m.series[logKey]
			if !ok {
				return
			}
			for ts := range samples {
				if _, ok := logSeries[ts]; !ok {
					return
				}
				delete(logSeries, ts)
			}
			if len(logSeries) == 0 {
				delete(m.series, logKey)
			}
		}

		for _, log := range logsToBeFlushed {
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

		for ts := range collectFlushedTimestamps(seriesToFlush) {
			m.skipList.Delete(ts)
		}

		m.totalLogs -= logsCount
	}()

	return nil
}

func collectFlushedTimestamps(series map[LogKey]map[int64]CounterValue) map[int64]struct{} {
	timestamps := make(map[int64]struct{})
	for _, samples := range series {
		for ts := range samples {
			timestamps[ts] = struct{}{}
		}
	}
	return timestamps
}

func (m *MemoryStore) Close() error {
	m.stopOnce.Do(func() {
		close(m.shutdown)
	})

	// wait for flush timers to return
	for i := 0; i < 2; i++ {
		<-m.done
	}

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
	ticker := time.NewTicker(m.maxTimeInMemory)
	defer func() {
		ticker.Stop()
		m.done <- struct{}{}
	}()

	for {
		select {
		case <-ticker.C:
			m.Flush(
				FlushConfig{
					startTs:        0,
					endTs:          0,
					MaxLogsToFlush: 0,
				},
			)
		case <-m.shutdown:
			if m.flushOnExit {
				m.Flush(
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

func (m *MemoryStore) startBackoffResetTimer() {
	timer := time.NewTimer(getAdjustedDuration())
	defer func() {
		timer.Stop()
		m.done <- struct{}{}
	}()

	for {
		select {
		case <-timer.C:
			m.backoff = getInitialBackoff(m.maxLogsInMem)
			timer.Reset(getAdjustedDuration())
		case <-m.shutdown:
			timer.Stop()
			return
		case <-m.maxLogReached:
			if !timer.Stop() {
				<-timer.C // concurrent firing case, drain this value
			}
			timer.Reset(getAdjustedDuration())
			m.Flush(
				FlushConfig{
					startTs:        0,
					endTs:          math.MaxInt64,
					MaxLogsToFlush: 1 << (m.backoff + 1),
				},
			)
			atomic.AddInt32(&m.backoff, 1)
		}
	}
}

func getInitialBackoff(maxLogsInMem int64) int32 {
	return int32(math.Log2(float64((maxLogsInMem + 4 - 1) / 4)))
}

func getAdjustedDuration() time.Duration {
	return 30*time.Minute + time.Duration(rand.Intn(20))*time.Minute
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
