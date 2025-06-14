package store

import (
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
)

const numShards = 16
const sep = byte(0xff)

type ShardedLogIndex struct {
	shards [numShards]struct {
		mu   sync.Mutex
		data map[LogKey]*CounterValue
	}
}

func NewShardedLogIndex() *ShardedLogIndex {
	s := &ShardedLogIndex{}
	for i := 0; i < numShards; i++ {
		s.shards[i].data = make(map[LogKey]*CounterValue)
	}
	return s
}

func HashLogKey(key LogKey) uint64 {
	b := make([]byte, 0, 1024)
	labels := []string{key.Service, key.Level, key.Message}
	for i, v := range labels {
		if len(b)+len(v)+1 >= cap(b) {
			h := xxhash.New()
			_, _ = h.Write(b)
			for _, v := range labels[i:] {
				_, _ = h.WriteString(v)
				_, _ = h.Write([]byte{sep})
			}

			return h.Sum64()
		}
		b = append(b, v...)
		b = append(b, sep)
	}

	return xxhash.Sum64(b)
}

func (s *ShardedLogIndex) getShard(key LogKey) *struct {
	mu   sync.Mutex
	data map[LogKey]*CounterValue
} {
	hash := HashLogKey(key) % numShards
	return &s.shards[hash]
}

func (s *ShardedLogIndex) Inc(key LogKey) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	cv, ok := shard.data[key]
	if !ok {
		cv = &CounterValue{}
		shard.data[key] = cv
	}
	atomic.AddUint64(&cv.value, 1)
}
