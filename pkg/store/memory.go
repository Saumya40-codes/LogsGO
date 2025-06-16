package store

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"sync"
	"time"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"github.com/Saumya40-codes/LogsGO/pkg"
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
	series          map[LogKey]map[int64]*CounterValue
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
		series:          make(map[LogKey]map[int64]*CounterValue),
		index:           index,
		meta: Labels{
			Services: make(map[string]int),
			Levels:   make(map[string]int),
		},
	}

	go mstore.startFlushTimer()
	return mstore
}

func (m *MemoryStore) Insert(logs []*logapi.LogEntry, _ map[LogKey]map[int64]*CounterValue) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, logs...)
	for _, log := range logs {
		key := LogKey{log.Service, log.Message, log.Level}
		ts := log.Timestamp
		m.index.Inc(key)
		shardedSeries := m.index.getShard(key)
		if m.series[key] == nil {
			m.series[key] = make(map[int64]*CounterValue)
		}
		newCounterVal := shardedSeries.data[key]
		m.series[key][ts] = newCounterVal
		m.meta.Services[log.Service]++
		m.meta.Levels[log.Level]++
	}
	return nil
}

func (m *MemoryStore) Query(parse LogFilter, lookback int64, qTime int64) ([]QueryResponse, error) {
	var results []QueryResponse
	if parse.LHS == nil && parse.RHS == nil {
		mlogs := slices.Clone(m.logs)
		slices.SortFunc(mlogs, func(a, b *logapi.LogEntry) int {
			if a.Timestamp < b.Timestamp {
				return 1
			}
			if a.Timestamp > b.Timestamp {
				return -1
			}
			return 0
		})

		for _, logs := range mlogs {
			// Skip if ts falls outside lookback
			if qTime-lookback > logs.Timestamp {
				continue
			}
			if parse.Service != "" && logs.Service != parse.Service {
				continue // Skip if service does not match
			}
			if parse.Level != "" && logs.Level != parse.Level {
				continue // Skip if level does not match
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

			results = append(results, QueryResponse{
				TimeStamp: logs.Timestamp,
				Service:   logs.Service,
				Level:     logs.Level,
				Message:   logs.Message,
				Count:     uint64(entry.value),
			})
		}
	} else if parse.Or {
		lhsResults, err := m.Query(*parse.LHS, lookback, qTime)
		if err != nil {
			return nil, fmt.Errorf("failed to query LHS: %w", err)
		}
		rhsResults, err := m.Query(*parse.RHS, lookback, qTime)
		if err != nil {
			return nil, fmt.Errorf("failed to query RHS: %w", err)
		}
		results = append(lhsResults, rhsResults...)
	} else {
		lhsResults, err := m.Query(*parse.LHS, lookback, qTime)
		if err != nil {
			return nil, fmt.Errorf("failed to query LHS: %w", err)
		}
		rhsResults, err := m.Query(*parse.RHS, lookback, qTime)
		if err != nil {
			return nil, fmt.Errorf("failed to query RHS: %w", err)
		}
		for _, lhsLog := range lhsResults {
			for _, rhsLog := range rhsResults {
				if lhsLog.Service == rhsLog.Service && lhsLog.Level == rhsLog.Level && lhsLog.TimeStamp == rhsLog.TimeStamp {
					results = append(results, lhsLog)
				}
			}
		}
	}

	if m.next != nil {
		if localStore, ok := (*m.next).(*LocalStore); ok {
			result, err := localStore.Query(parse, lookback, qTime)
			if err != nil {
				return nil, fmt.Errorf("failed to query next store: %w", err)
			}
			results = append(results, result...)
		} else {
			return nil, fmt.Errorf("next store is not a LocalStore, cannot query")
		}
	}

	results = deDuplicate(results)
	return results, nil
}

func (m *MemoryStore) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var logsToBeFlushed []*logapi.LogEntry
	var logsToKeep []*logapi.LogEntry
	for _, logs := range m.logs {
		if time.Now().Unix()-logs.Timestamp >= int64(m.maxTimeInMemory.Seconds()) { // TODO: think of some better scheduling, currently there might be case that logs just miss out by few seconds to be flushed and will be in memory for longer time
			logsToBeFlushed = append(logsToBeFlushed, logs)
		} else {
			logsToKeep = append(logsToKeep, logs)
		}
	}

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

func deDuplicate(res []QueryResponse) []QueryResponse {
	// only take latest timestamp into consideration for a query response
	sort.Slice(res, func(a, b int) bool {
		return res[a].TimeStamp > res[b].TimeStamp
	})

	vis := make(map[LogKey]struct{})

	dedupResponse := make([]QueryResponse, 0)

	for _, r := range res {
		key := LogKey{Service: r.Service, Level: r.Level, Message: r.Message}

		if _, ok := vis[key]; !ok {
			vis[key] = struct{}{}
			dedupResponse = append(dedupResponse, r)
		}
	}

	return dedupResponse
}
