package store

import logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"

// Store interfaces defines the methods that any store implementation should provide.
type Store interface {
	Query()
	Insert(logs []*logapi.LogEntry) error
	Flush() error
	Close() error
	LabelValues() (Labels, error) // Returns all the unique label values for services and levels
	// TODO: Find what else we need here
}

type Labels struct {
	Services []string
	Levels   []string
	// TODO: Add keywords or other labels if needed
}

type LogFilter struct {
	MinTimestamp int64
	MaxTimestamp int64
	Level        string
	Service      string
	Keyword      string
}

func Contains(slice []string, item string) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}
