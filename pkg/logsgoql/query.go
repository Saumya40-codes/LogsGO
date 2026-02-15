package logsgoql

import (
	"errors"
	"sync"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
)

type LogFilter struct {
	Level   string
	Service string
	Or      bool
	RHS     *LogFilter
	LHS     *LogFilter
}

var (
	ErrInvalidQuery   = errors.New("invalid_query")
	ErrNotFound       = errors.New("not_found")
	ErrInternal       = errors.New("internal_error")
	ErrNotImplemented = errors.New("not_implemented")
	ErrInvalidLabel   = errors.New("invalid_label")
)

type InstantQueryCache struct {
	LocalOnce sync.Once
	LocalKeys []string
	LocalVals []string
	LocalErr  error

	BucketOnce sync.Once
	BucketData []*logapi.Series
	BucketErr  error
}

// QueryConfig are relevant to one query life time, for instance fetching data from bucket only once during query lifetime
type RangeQueryConfig struct {
	Once       *sync.Once
	StartTs    int64
	EndTs      int64
	Lookback   int64
	Resolution int64
	Filter     LogFilter
}

type InstantQueryConfig struct {
	Ts       int64
	Lookback int64
	Filter   LogFilter
	Cache    *InstantQueryCache
}

func NewRangeQueryConfig(startTs, endTs, Lookback, Resolution int64, filter LogFilter) *RangeQueryConfig {
	return &RangeQueryConfig{
		Once:       &sync.Once{},
		StartTs:    startTs,
		EndTs:      endTs,
		Lookback:   Lookback,
		Resolution: Resolution,
		Filter:     filter,
	}
}

func NewInstantQueryConfig(ts, Lookback int64, filter LogFilter) *InstantQueryConfig {
	cache := &InstantQueryCache{
		LocalOnce: sync.Once{},
		LocalKeys: make([]string, 0),
		LocalVals: make([]string, 0),

		BucketOnce: sync.Once{},
		BucketData: make([]*logapi.Series, 0),
	}

	return &InstantQueryConfig{
		Ts:       ts,
		Lookback: Lookback,
		Filter:   filter,
		Cache:    cache,
	}
}
