package store

import (
	"errors"
	"fmt"
	"maps"
	"sort"
	"sync"
	"time"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"github.com/Saumya40-codes/LogsGO/pkg"
	"github.com/Saumya40-codes/LogsGO/pkg/logsgoql"
)

// MemoryStore implements the Store interface using an in-memory map.
type MemoryStore struct {
	logs            []*logapi.LogEntry
	mu              sync.Mutex
	next            *Store        // Next store in the chain, if any
	maxTimeInMemory time.Duration // Maximum time in memory, after which logs are flushed to the next store
	lastFlushTime   int64         // Timestamp of the last flush operation
	shutdown        chan struct{} // Channel to signal shutdown of the store
	flushOnExit     bool
	series          map[LogKey]map[int64]CounterValue
	index           *ShardedLogIndex // shared log index
	meta            Labels           // contains all unique labels for this store, this is used to get unique label values
}

func NewMemoryStore(next *Store, maxTimeInMemory string, flushOnExit bool, index *ShardedLogIndex) *MemoryStore {
	mstore := &MemoryStore{
		logs:            make([]*logapi.LogEntry, 0),
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
	}

	go mstore.startFlushTimer()
	return mstore
}

func (m *MemoryStore) Insert(logs []*logapi.LogEntry, _ map[LogKey]map[int64]CounterValue) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, logs...)

	for _, log := range logs {
		key := LogKey{log.Service, log.Message, log.Level}
		ts := log.Timestamp
		m.index.Inc(key)
		shardedSeries := m.index.getShard(key)
		if m.series[key] == nil {
			m.series[key] = make(map[int64]CounterValue)
		}
		newCounterVal := shardedSeries.data[key]
		m.series[key][ts] = *newCounterVal
		m.meta.Services[log.Service]++
		m.meta.Levels[log.Level]++
	}

	sort.Slice(m.logs, func(i, j int) bool {
		return m.logs[j].Timestamp < m.logs[i].Timestamp
	})
	return nil
}

func (m *MemoryStore) QueryInstant(cfg *logsgoql.InstantQueryConfig) ([]InstantQueryResponse, error) {
	var allResults []InstantQueryResponse

	if cfg.Filter.LHS == nil && cfg.Filter.RHS == nil {
		startIdx := getStartingTimeIndex(m.logs, cfg.Ts)

		for idx := startIdx; idx < len(m.logs); idx++ {
			logs := m.logs[idx]
			if cfg.Ts-cfg.Lookback > logs.Timestamp {
				break
			}
			if logs.Timestamp > cfg.Ts { // this won't be needed as we only consider logs with Ts <= cfg.Ts, but lets keep it for sanity
				continue
			}
			if cfg.Filter.Service != "" && logs.Service != cfg.Filter.Service {
				continue
			}
			if cfg.Filter.Level != "" && logs.Level != cfg.Filter.Level {
				continue
			}

			logKey := LogKey{Service: logs.Service, Level: logs.Level, Message: logs.Message}
			logSeries, ok := m.series[logKey]
			if !ok {
				return nil, fmt.Errorf("logKey %v not found in series", logKey)
			}
			entry, ok := logSeries[logs.Timestamp]
			if !ok {
				return nil, fmt.Errorf("timestamp %v not found in logKey series", logs.Timestamp)
			}

			allResults = append(allResults, InstantQueryResponse{
				TimeStamp: logs.Timestamp,
				Service:   logs.Service,
				Level:     logs.Level,
				Message:   logs.Message,
				Count:     uint64(entry.value),
			})
		}
	} else {
		lhsResults := make([]InstantQueryResponse, 0)
		var err error
		if cfg.Filter.LHS != nil {
			lhsCfg := *cfg
			lhsCfg.Filter = *cfg.Filter.LHS
			lhsResults, err = m.QueryInstant(&lhsCfg)
			if err != nil {
				return nil, fmt.Errorf("failed to query LHS: %w", err)
			}
		}

		rhsResults := make([]InstantQueryResponse, 0)
		if cfg.Filter.RHS != nil {
			rhsCfg := *cfg
			rhsCfg.Filter = *cfg.Filter.RHS
			rhsResults, err = m.QueryInstant(&rhsCfg)
			if err != nil {
				return nil, fmt.Errorf("failed to query RHS: %w", err)
			}
		}

		if cfg.Filter.Or {
			allResults = append(allResults, lhsResults...)
			allResults = append(allResults, rhsResults...)
		} else {
			rhsMap := make(map[string]struct{})
			for _, r := range rhsResults {
				key := r.Service + "|" + r.Level + "|" + r.Message
				rhsMap[key] = struct{}{}
			}
			for _, r := range lhsResults {
				key := r.Service + "|" + r.Level + "|" + r.Message
				if _, ok := rhsMap[key]; ok {
					allResults = append(allResults, r)
				}
			}
		}
	}

	// Merge results from the next store
	if m.next != nil {
		if localStore, ok := (*m.next).(*LocalStore); ok {
			nextResults, err := localStore.QueryInstant(cfg)
			if err != nil {
				return nil, fmt.Errorf("failed to query next store: %w", err)
			}
			allResults = append(allResults, nextResults...)
		} else {
			return nil, fmt.Errorf("next store is not a LocalStore, cannot query")
		}
	}

	return deduplicateInstantResponses(allResults), nil
}

// QueryRange is Instant Query, which is done at several intervals based on the resolution
func (m *MemoryStore) QueryRange(cfg *logsgoql.RangeQueryConfig) ([]QueryResponse, error) {
	temp := make(map[LogKey][]Series)
	newInstantCfg := logsgoql.NewInstantQueryConfig(0, cfg.Lookback, cfg.Filter)

	for t := cfg.StartTs; t <= cfg.EndTs; t += cfg.Resolution {
		newInstantCfg.Ts = t
		instantResults, err := m.QueryInstant(newInstantCfg)
		if err != nil {
			return nil, fmt.Errorf("failed to query logs: %w", err)
		}
		for _, log := range instantResults {
			key := LogKey{Service: log.Service, Level: log.Level, Message: log.Message}
			temp[key] = append(temp[key], Series{
				Timestamp: t,
				Count:     log.Count,
			})
		}
	}

	results := make([]QueryResponse, 0, len(temp))
	for key, series := range temp {
		results = append(results, QueryResponse{
			Level:   key.Level,
			Service: key.Service,
			Message: key.Message,
			Series:  series,
		})
	}

	return results, nil
}

func (m *MemoryStore) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var logsToBeFlushed []*logapi.LogEntry
	var logsToKeep []*logapi.LogEntry
	currT := time.Now().Unix()
	maxThreshold := currT - int64(m.maxTimeInMemory.Seconds())

	// YOLO
	if maxThreshold < 0 {
		return errors.New("Invalid maxTimeInMemory parameter set")
	}

	thresholdIdx := getStartingTimeIndex(m.logs, maxThreshold)
	logsToBeFlushed = m.logs[thresholdIdx:]
	logsToKeep = m.logs[:thresholdIdx]

	if len(logsToBeFlushed) == 0 {
		return nil // No logs to flush
	}

	if m.next != nil {
		if localStore, ok := (*m.next).(*LocalStore); ok {
			if err := localStore.Insert(logsToBeFlushed, m.series); err != nil {
				return fmt.Errorf("failed to insert logs into next store: %w", err)
			}
		} else {
			return fmt.Errorf("next store is not a LocalStore, cannot insert logs")
		}
	}

	m.logs = logsToKeep

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

	m.logs = m.logs[:0]

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

func deduplicateInstantResponses(responses []InstantQueryResponse) []InstantQueryResponse {
	deduped := make(map[string]InstantQueryResponse)

	for _, r := range responses {
		key := r.Service + "|" + r.Level + "|" + r.Message
		if _, exists := deduped[key]; !exists {
			deduped[key] = r
		}
	}

	final := make([]InstantQueryResponse, 0, len(deduped))
	for _, v := range deduped {
		final = append(final, v)
	}

	sort.Slice(final, func(i, j int) bool {
		return final[i].TimeStamp < final[j].TimeStamp
	})

	return final
}
