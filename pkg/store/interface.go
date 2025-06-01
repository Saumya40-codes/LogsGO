package store

import (
	"errors"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
)

// Store interfaces defines the methods that any store implementation should provide.
type Store interface {
	Query(filter LogFilter) ([]*logapi.LogEntry, error) // Returns logs matching the filter
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
	Level   string
	Service string
	Or      bool
	RHS     *LogFilter
	LHS     *LogFilter
}

func Contains(slice []string, item string) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

var (
	ErrInvalidQuery   = errors.New("invalid_query")
	ErrNotFound       = errors.New("not_found")
	ErrInternal       = errors.New("internal_error")
	ErrNotImplemented = errors.New("not_implemented")
	ErrInvalidLabel   = errors.New("invalid_label")
)
