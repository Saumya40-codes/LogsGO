package store

import logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"

// Store interfaces defines the methods that any store implementation should provide.
type Store interface {
	Query()
	Insert(logs []*logapi.LogEntry) error
	Flush() error
	Close() error
	// TODO: Find what else we need here
}

type LogFilter struct {
	MinTimestamp int64
	MaxTimestamp int64
	Level        string
	Service      string
	Keyword      string
}
