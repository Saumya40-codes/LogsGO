package store

// BlockInfo contains info regrading time range and level of compaction the block holds
type BlockInfo struct {
	ObjectKey string
	Service   string
	MinTs     int64
	MaxTs     int64
	Level     int
}

// CompactConfig contains durations related to compaction trigger and downsampling, the durations are in UNIX format
type CompactConfig struct {
	Level0Retention int64 // How long to keep 2h blocks before compacting to 12h
	Level1Retention int64 // How long to keep 12h blocks before compacting to 24h
	Level2Retention int64 // How long to keep 24h blocks before scheduling it for deletion

	// Downsampling intervals
	Level1Interval int64 // Original interval (e.g., 1s)
	Level2Interval int64 // First downsampling (e.g., 1m)
	Level3Interval int64 // Second downsampling (e.g., 5m)

	// Compaction batch sizes
	Level1BatchSize int // Number of 2h blocks to merge into 12h
	Level2BatchSize int // Number of 12h blocks to merge into 24h
}
