package store

import (
	"fmt"
	"slices"
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
}

func (c *CounterValue) Inc() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value++
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

			return results, nil // we found the nearest value so return
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
			// Remove the logs that are flushed
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

func mapToLogEntrys(logs map[string]map[string]string, ts int64) []*logapi.LogEntry {
	var entries []*logapi.LogEntry
	for service, levels := range logs {
		for level, message := range levels {
			entries = append(entries, &logapi.LogEntry{
				Timestamp: ts,
				Service:   service,
				Level:     level,
				Message:   message,
			})
		}
	}
	return entries
}

// LabelValues returns the unique label values from the local store. We will have chain of stores, so this will return the unique values from all the stores in the chain.
func (m *MemoryStore) LabelValues(labels *Labels) error {
	for _, logs := range m.logs {
		labels.Services[logs.Service] = struct{}{}
		labels.Levels[logs.Level] = struct{}{}
	}

	if localStore, ok := (*m.next).(*LocalStore); ok {
		err := localStore.LabelValues(labels)
		if err != nil {
			return fmt.Errorf("failed to get label values from local store: %w", err)
		}
	}

	return nil
}
