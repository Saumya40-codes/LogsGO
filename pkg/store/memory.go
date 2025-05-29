package store

import (
	"sync"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
)

// MemoryStore implements the Store interface using an in-memory map.
type MemoryStore struct {
	logs map[string]map[string][]*logapi.LogEntry // TODO: maps are costly, can we employ some hashing and use slice instead?
	mu   sync.Mutex
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		logs: make(map[string]map[string][]*logapi.LogEntry),
	}
}

func (m *MemoryStore) Insert(logs []*logapi.LogEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, log := range logs {
		service := log.Service
		level := log.Level
		if _, exists := m.logs[service]; !exists {
			m.logs[service] = make(map[string][]*logapi.LogEntry)
		}
		m.logs[service][level] = append(m.logs[service][level], log)
	}
}

func (m *MemoryStore) Query(filter LogFilter) []*logapi.LogEntry {
	m.mu.Lock()
	defer m.mu.Unlock()

	var results []*logapi.LogEntry
	for service, levels := range m.logs {
		if filter.Service != "" && service != filter.Service {
			continue
		}
		for level, entries := range levels {
			if filter.Level != "" && level != filter.Level {
				continue
			}
			results = append(results, entries...) // TODO: filter by timestamp and keyword
		}
	}
	return results
}

func (m *MemoryStore) Flush() error {
	// Ideally, in-memory should flush to a persistent store. We have BadgerDB for that.
	// Lets return nil for now.
	// Should we have this method in the interface? As if we need to flush it to peristance store, we can do it in the Insert method itself.
	// During Shutdown, we can call this method to ensure all logs are persisted.
	return nil
}

func (m *MemoryStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.logs = make(map[string]map[string][]*logapi.LogEntry)
	return nil
}
