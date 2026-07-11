package store

import (
	"fmt"
	"maps"
	"math"
	"sync"
	"time"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"github.com/Saumya40-codes/LogsGO/pkg"
	"github.com/Saumya40-codes/LogsGO/pkg/internal"
	"github.com/Saumya40-codes/LogsGO/pkg/logsgoql"
	"github.com/Saumya40-codes/LogsGO/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

const defaultEvictionInterval = time.Minute

// MemoryStore is a write-through, policy-driven read cache. Every log is
// forwarded to the next (durable) store; only logs matching the cache policy
// are additionally retained here to accelerate hot queries.
type MemoryStore struct {
	mu         sync.RWMutex
	stopOnce   sync.Once
	next       *Store
	ttl        time.Duration
	maxEntries int64
	policy     *CachePolicy
	shutdown   chan struct{}
	skipList   *internal.SkipList
	series     map[LogKey]map[int64]CounterValue
	index      *ShardedLogIndex
	meta       Labels
	metrics    *metrics.Metrics
	totalLogs  int64
	done       chan struct{}
}

func NewMemoryStore(next *Store, maxTimeInMemory string, maxLogsInMem int64, policy *CachePolicy, index *ShardedLogIndex, metrics *metrics.Metrics) *MemoryStore {
	ttl := pkg.GetTimeDuration(maxTimeInMemory)
	maxEntries := maxLogsInMem
	if policy != nil {
		if policy.ttl > 0 {
			ttl = policy.ttl
		}
		if policy.maxEntries > 0 {
			maxEntries = policy.maxEntries
		}
	}

	mstore := &MemoryStore{
		next:       next,
		ttl:        ttl,
		maxEntries: maxEntries,
		policy:     policy,
		shutdown:   make(chan struct{}),
		series:     make(map[LogKey]map[int64]CounterValue),
		index:      index,
		meta:       emptyLabels(),
		metrics:    metrics,
		done:       make(chan struct{}, 1),
	}

	mstore.skipList = internal.NewSkipList()

	go mstore.startEvictionTimer()
	return mstore
}

func (m *MemoryStore) Insert(logs []*logapi.LogEntry, _ map[LogKey]map[int64]CounterValue, _ string) error {
	m.mu.Lock()

	timer := prometheus.NewTimer(m.metrics.IngestionDuration.WithLabelValues("memory"))

	fullSeries := make(map[LogKey]map[int64]CounterValue, len(logs))
	toCache := make([]*logapi.LogEntry, 0, len(logs))

	for _, lg := range logs {
		customLabels := normalizeCustomLabels(lg.Labels)
		key := LogKey{Service: lg.Service, Message: lg.Message, Level: lg.Level, CustomLabels: labelsFingerprint(customLabels)}
		ts := lg.Timestamp

		m.index.Inc(key)
		cv := *m.index.getShard(key).data[key]
		if fullSeries[key] == nil {
			fullSeries[key] = make(map[int64]CounterValue)
		}
		fullSeries[key][ts] = cv

		if m.policy.ShouldCache(logsgoql.EntryLabels{Service: lg.Service, Level: lg.Level, Message: lg.Message, Labels: customLabels}) {
			toCache = append(toCache, lg)
		}
	}

	floor := m.oldestKeyLocked()
	enforceFloor := m.totalLogs > 0
	for _, lg := range toCache {
		ts := lg.Timestamp
		if enforceFloor && ts < floor {
			continue // below the covered window; pebble still has it
		}
		customLabels := normalizeCustomLabels(lg.Labels)
		key := LogKey{Service: lg.Service, Message: lg.Message, Level: lg.Level, CustomLabels: labelsFingerprint(customLabels)}
		if m.series[key] == nil {
			m.series[key] = make(map[int64]CounterValue)
		}
		_, already := m.series[key][ts]
		m.series[key][ts] = fullSeries[key][ts]
		if already {
			// Count-only update: keep one skiplist entry per (series, ts) so
			// reads do not walk one Value per ingested log.
			continue
		}
		m.skipList.Insert(ts, internal.Value{Service: lg.Service, Level: lg.Level, Message: lg.Message, Labels: cloneLabels(customLabels)})
		incMetaLabels(&m.meta, lg.Service, lg.Level, customLabels)
		m.totalLogs++
	}

	if m.maxEntries > 0 {
		m.evictOverflowLocked()
	}

	m.metrics.LogsIngested.WithLabelValues("memory").Add(float64(len(toCache)))
	timer.ObserveDuration()
	m.mu.Unlock()

	if m.next != nil && len(logs) > 0 {
		if err := (*m.next).Insert(logs, fullSeries, ""); err != nil {
			return fmt.Errorf("failed to insert logs into next store: %w", err)
		}
	}
	return nil
}

func (m *MemoryStore) Series(queryCtx logsgoql.QueryContext, plan *logsgoql.Plan) ([]logsgoql.Series, error) {
	next := m.next
	if m.covers(queryCtx, plan) {
		next = nil
	}
	return tieredSeries(queryCtx, plan, 0, next, func() ([]logsgoql.Series, error) {
		m.mu.RLock()
		defer m.mu.RUnlock()
		return m.getSeries(queryCtx, plan)
	})
}

func (m *MemoryStore) SeriesRange(queryCtx logsgoql.QueryContext, plan *logsgoql.Plan, resolution int64) ([]logsgoql.Series, error) {
	next := m.next
	if m.covers(queryCtx, plan) {
		next = nil
	}
	return tieredSeries(queryCtx, plan, resolution, next, func() ([]logsgoql.Series, error) {
		m.mu.RLock()
		defer m.mu.RUnlock()
		return m.getSeries(queryCtx, plan)
	})
}

// covers reports whether the cache alone can answer the query, letting us skip
// the durable tier. It requires both a policy that subsumes the query and a
// retained window that spans the query's start.
func (m *MemoryStore) covers(queryCtx logsgoql.QueryContext, plan *logsgoql.Plan) bool {
	if !m.policy.Covers(plan) {
		return false
	}
	start, _ := seriesIterWindow(queryCtx)

	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.totalLogs == 0 {
		return false
	}
	return start >= m.oldestKeyLocked()
}

func (m *MemoryStore) oldestKeyLocked() int64 {
	node, ok := m.skipList.Seek(internal.IteratorSearchOpts{Start: 0}).Next()
	if !ok {
		return math.MaxInt64
	}
	return node.GetKey()
}

func (m *MemoryStore) evictOverflowLocked() {
	for m.totalLogs > m.maxEntries {
		node, ok := m.skipList.Seek(internal.IteratorSearchOpts{Start: 0}).Next()
		if !ok {
			return
		}
		m.evictTimestampLocked(node.GetKey(), node.GetValues())
	}
}

func (m *MemoryStore) evictTimestampLocked(ts int64, values []internal.Value) {
	for _, v := range values {
		customLabels := normalizeCustomLabels(v.Labels)
		key := LogKey{Service: v.Service, Level: v.Level, Message: v.Message, CustomLabels: labelsFingerprint(customLabels)}
		if samples, ok := m.series[key]; ok {
			delete(samples, ts)
			if len(samples) == 0 {
				delete(m.series, key)
			}
		}
		decMetaLabels(&m.meta, v.Service, v.Level, customLabels)
		m.totalLogs--
	}
	m.skipList.Delete(ts)
}

func (m *MemoryStore) evictExpired() {
	if m.ttl <= 0 {
		return
	}
	cutoff := time.Now().Unix() - int64(m.ttl.Seconds())
	if cutoff <= 0 {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	it := m.skipList.Seek(internal.IteratorSearchOpts{Start: 0, End: cutoff})
	var expired []*internal.Node
	for {
		node, ok := it.Next()
		if !ok {
			break
		}
		expired = append(expired, node)
	}
	for _, node := range expired {
		m.evictTimestampLocked(node.GetKey(), node.GetValues())
	}
}

// Flush is a no-op: the cache never flushes downward since every log is already
// written through to the durable store on insert. It exists to satisfy Store.
func (m *MemoryStore) Flush(_ FlushConfig) error {
	return nil
}

func (m *MemoryStore) Close() error {
	m.stopOnce.Do(func() {
		close(m.shutdown)
	})

	<-m.done

	m.mu.Lock()
	m.series = make(map[LogKey]map[int64]CounterValue)
	m.skipList = internal.NewSkipList()
	m.meta = emptyLabels()
	m.totalLogs = 0
	m.mu.Unlock()

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

func (m *MemoryStore) startEvictionTimer() {
	interval := m.ttl
	if interval <= 0 || interval > defaultEvictionInterval {
		interval = defaultEvictionInterval
	}
	ticker := time.NewTicker(interval)
	defer func() {
		ticker.Stop()
		m.done <- struct{}{}
	}()

	for {
		select {
		case <-ticker.C:
			m.evictExpired()
		case <-m.shutdown:
			return
		}
	}
}

// LabelValues returns the unique label values from this store's cache merged
// with the durable stores below it.
func (m *MemoryStore) LabelValues(labels *Labels) error {
	m.mu.RLock()
	*labels = emptyLabels()
	maps.Copy(labels.Services, m.meta.Services)
	maps.Copy(labels.Levels, m.meta.Levels)
	for label, values := range m.meta.CustomLabels {
		labels.CustomLabels[label] = make(map[string]int)
		maps.Copy(labels.CustomLabels[label], values)
	}
	m.mu.RUnlock()

	if m.next != nil {
		if localStore, ok := (*m.next).(*LocalStore); ok {
			if err := localStore.LabelValues(labels); err != nil {
				return fmt.Errorf("failed to get label values from local store: %w", err)
			}
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
			customLabels := normalizeCustomLabels(v.Labels)
			logKey := LogKey{Service: v.Service, Level: v.Level, Message: v.Message, CustomLabels: labelsFingerprint(customLabels)}
			if _, seen := seenThisTs[logKey]; seen {
				continue
			}
			seenThisTs[logKey] = struct{}{}

			matched, err := plan.Match(logsgoql.EntryLabels{
				Service: v.Service,
				Level:   v.Level,
				Message: v.Message,
				Labels:  customLabels,
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
			Labels:  labelsFromFingerprint(k.CustomLabels),
			Points:  pts,
		})
	}

	return results, nil
}
