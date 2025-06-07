package store

import (
	"fmt"
	"sync"
	"time"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"github.com/Saumya40-codes/LogsGO/pkg"
)

// MemoryStore implements the Store interface using an in-memory map.
type MemoryStore struct {
	logs            map[int64]map[string]map[string]string // TODO: maps are costly, can we employ some hashing and use slice instead?
	mu              sync.Mutex
	next            *Store        // Next store in the chain, if any
	maxTimeInMemory time.Duration // Maximum time in memory, after which logs are flushed to the next store
	lastFlushTime   int64         // Timestamp of the last flush operation
	shutdown        chan struct{} // Channel to signal shutdown of the store
	flushOnExit     bool
}

func NewMemoryStore(next *Store, maxTimeInMemory string, flushOnExit bool) *MemoryStore {
	mstore := &MemoryStore{
		logs:            make(map[int64]map[string]map[string]string), // map[timestamp][service][level] = message
		mu:              sync.Mutex{},
		next:            next,
		maxTimeInMemory: pkg.GetTimeDuration(maxTimeInMemory),
		lastFlushTime:   0,
		shutdown:        make(chan struct{}),
		flushOnExit:     flushOnExit,
	}

	go mstore.startFlushTimer()
	return mstore
}

func (m *MemoryStore) Insert(logs []*logapi.LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, log := range logs {
		service := log.Service
		level := log.Level
		ts := log.Timestamp

		if _, exists := m.logs[ts]; !exists {
			m.logs[ts] = make(map[string]map[string]string)
		}
		if _, exists := m.logs[ts][service]; !exists {
			m.logs[ts][service] = make(map[string]string)
		}
		m.logs[ts][service][level] = log.Message
	}

	return nil // No error, insertion successful
}

func (m *MemoryStore) Query(parse LogFilter) ([]*logapi.LogEntry, error) {
	var results []*logapi.LogEntry
	if parse.LHS == nil && parse.RHS == nil {
		for ts, logs := range m.logs {
			for service, levels := range logs {
				if parse.Service != "" && service != parse.Service {
					continue // Skip if service does not match
				}
				for level, message := range levels {
					if parse.Level != "" && level != parse.Level {
						continue // Skip if level does not match
					}
					results = append(results, &logapi.LogEntry{
						Timestamp: ts,
						Service:   service,
						Level:     level,
						Message:   message,
					})
				}
			}
		}

	} else if parse.Or {
		lhsResults, err := m.Query(*parse.LHS)
		if err != nil {
			return nil, fmt.Errorf("failed to query LHS: %w", err)
		}
		rhsResults, err := m.Query(*parse.RHS)
		if err != nil {
			return nil, fmt.Errorf("failed to query RHS: %w", err)
		}
		results = append(lhsResults, rhsResults...)
	} else {
		lhsResults, err := m.Query(*parse.LHS)
		if err != nil {
			return nil, fmt.Errorf("failed to query LHS: %w", err)
		}
		rhsResults, err := m.Query(*parse.RHS)
		if err != nil {
			return nil, fmt.Errorf("failed to query RHS: %w", err)
		}
		for _, lhsLog := range lhsResults {
			for _, rhsLog := range rhsResults {
				if lhsLog.Service == rhsLog.Service && lhsLog.Level == rhsLog.Level && lhsLog.Timestamp == rhsLog.Timestamp {
					results = append(results, lhsLog)
				}
			}
		}
	}

	if m.next != nil {
		if localStore, ok := (*m.next).(*LocalStore); ok {
			result, err := localStore.Query(parse)
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
	for ts, logs := range m.logs {
		if time.Now().Unix()-ts >= int64(m.maxTimeInMemory.Seconds()) { // TODO: think of some better scheduling, currently there might be case that logs just miss out by few seconds to be flushed and will be in memory for longer time
			entry := mapToLogEntrys(logs, ts)
			logsToBeFlushed = append(logsToBeFlushed, entry...)
			// Remove the logs that are flushed
			delete(m.logs, ts)
		}
	}

	if len(logsToBeFlushed) == 0 {
		return nil // No logs to flush
	}

	if m.next != nil {
		if localStore, ok := (*m.next).(*LocalStore); ok {
			if err := localStore.Insert(logsToBeFlushed); err != nil {
				return fmt.Errorf("failed to insert logs into next store: %w", err)
			}
		} else {
			return fmt.Errorf("next store is not a LocalStore, cannot insert logs")
		}
	}

	return nil
}

func (m *MemoryStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.logs = make(map[int64]map[string]map[string]string) // Clear the logs

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
		for service := range logs {
			labels.Services[service] = struct{}{}
			for level := range logs[service] {
				labels.Levels[level] = struct{}{}
			}
		}
	}

	if localStore, ok := (*m.next).(*LocalStore); ok {
		err := localStore.LabelValues(labels)
		if err != nil {
			return fmt.Errorf("failed to get label values from local store: %w", err)
		}
	}

	return nil
}
