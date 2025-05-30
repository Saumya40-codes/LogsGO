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
	maxTimeInMemory int64         // Maximum time in memory, after which logs are flushed to the next store
	lastFlushTime   int64         // Timestamp of the last flush operation
	shutdown        chan struct{} // Channel to signal shutdown of the store
}

func NewMemoryStore(next *Store, maxTimeInMemory string) *MemoryStore {
	mstore := &MemoryStore{
		logs:            make(map[int64]map[string]map[string]string), // map[timestamp][service][level] = message
		mu:              sync.Mutex{},
		next:            next,
		maxTimeInMemory: int64(pkg.GetTimeDuration(maxTimeInMemory).Seconds()),
		lastFlushTime:   0,
		shutdown:        make(chan struct{}),
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

func (m *MemoryStore) Query() {
	// TODO: Implement query logic based on the filter
}

func (m *MemoryStore) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var logsToBeFlushed []*logapi.LogEntry
	for ts, logs := range m.logs {
		if time.Now().Unix()-ts >= m.maxTimeInMemory { // TODO: think of some better scheduling, currently there might be case that logs just miss out by few seconds to be flushed and will be in memory for longer time
			entry := mapToLogEntrys(logs, ts)
			logsToBeFlushed = append(logsToBeFlushed, entry...)
			// Remove the logs that are flushed
			delete(m.logs, ts) // Remove the logs that are flushed
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
	return nil
}

func (m *MemoryStore) startFlushTimer() {
	for {
		select {
		case <-time.After(time.Duration(m.maxTimeInMemory)):
			// Start Flushing, what to flush should be decided by another function
			m.Flush()
			m.lastFlushTime = time.Now().Unix()
		case <-m.shutdown:
			m.Flush() // TODO: Is this correect? Should we flush all logs before closing?
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
