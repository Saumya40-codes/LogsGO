package logsgoql

type QueryContext struct {
	StartTs  int64
	EndTs    int64
	Lookback int64
}

type Sample struct {
	Timestamp int64
	Count     uint64
}

type Series struct {
	Service string
	Level   string
	Message string
	Points  []Sample
}

// Each store should implement as Series() method
type Store interface {
	Series(ctx QueryContext, plan *Plan) ([]Series, error)
	SeriesRange(ctx QueryContext, plan *Plan, resolution int64) ([]Series, error)
}

type Engine struct {
	// store is the "head" store for query execution.
	//
	// In a tiered setup (mem -> local -> bucket), the head store is responsible
	// for querying downstream tiers (if any) and reconciling results to avoid
	// duplicates during flush overlap windows.
	store Store
}

func NewEngine(store Store) Engine {
	return Engine{store: store}
}
