package store

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"github.com/minio/minio-go/v7"
)

// CompactConfig contains durations related to compaction trigger and downsampling
type CompactConfig struct {
	Level0Retention time.Duration `yaml:"level0_retention"` // How long to keep 2h blocks before compacting to 12h
	Level1Retention time.Duration `yaml:"level1_retention"` // How long to keep 12h blocks before compacting to 24h
	Level2Retention time.Duration `yaml:"level2_retention"` // How long to keep 24h blocks before scheduling it for deletion

	// Maximum block lifetime - blocks older than this will be deleted regardless of level
	MaxBlockLifetime time.Duration `yaml:"max_block_lifetime"`

	// Grace period to keep old blocks after compaction (for safety)
	CompactionGracePeriod time.Duration `yaml:"compaction_grace_period"`

	// Downsampling intervals (TODO)
	Level1Interval int64 `yaml:"level1_interval"` // Original interval (e.g., 1s)
	Level2Interval int64 `yaml:"level2_interval"` // First downsampling (e.g., 1m)
	Level3Interval int64 `yaml:"level3_interval"` // Second downsampling (e.g., 5m)

	// Compaction batch sizes
	Level1BatchSize int `yaml:"level1_batch_size"` // Number of 2h blocks to merge into 12h
	Level2BatchSize int `yaml:"level2_batch_size"` // Number of 12h blocks to merge into 24h

	// Allow incomplete batches for compaction
	AllowIncompleteBatches bool `yaml:"allow_incomplete_batches"`

	// Maximum time gap allowed between consecutive blocks in a batch (in seconds)
	MaxTimeGapBetweenBlocks int64 `yaml:"max_time_gap_between_blocks"`
}

type BlockInfo struct {
	ObjectKey    string
	Service      string
	MinTs        int64
	MaxTs        int64
	Level        int // 0 = 2h, 1 = 12h, 2 = 24h
	LastModified time.Time
	// Track if this block was created by compaction
	IsCompacted bool
	// Track when this block was created (for grace period)
	CreatedAt time.Time
}

// CompactionResult tracks the results of a compaction operation
type CompactionResult struct {
	BlocksCompacted  int
	BlocksDeleted    int
	NewBlocksCreated int
	Errors           []error
}

// Run Compaction is a method that triggers the compaction process for the BucketStore with default configuration.
func (b *BucketStore) RunCompaction() {
	config := CompactConfig{
		Level0Retention:         24 * time.Hour,      // 2h blocks kept for 24h
		Level1Retention:         7 * 24 * time.Hour,  // 12h blocks kept for 7 days
		Level2Retention:         30 * 24 * time.Hour, // 24h blocks kept for 30 days
		MaxBlockLifetime:        90 * 24 * time.Hour, // Maximum 90 days for any block
		CompactionGracePeriod:   2 * time.Hour,       // Keep old blocks for 2h after compaction
		Level1BatchSize:         6,                   // 6 blocks of 2h to make 12h
		Level2BatchSize:         2,                   // 2 blocks of 12h to make 24h
		AllowIncompleteBatches:  false,               // Only compact complete batches by default
		MaxTimeGapBetweenBlocks: 3600,                // Max 1 hour gap between blocks
	}

	result := b.runCompactionCycle(config)
	if len(result.Errors) > 0 {
		log.Printf("Compaction completed with %d errors. Blocks compacted: %d, deleted: %d, created: %d",
			len(result.Errors), result.BlocksCompacted, result.BlocksDeleted, result.NewBlocksCreated)
		for _, err := range result.Errors {
			log.Printf("Compaction error: %v", err)
		}
	} else {
		log.Printf("Compaction completed successfully. Blocks compacted: %d, deleted: %d, created: %d",
			result.BlocksCompacted, result.BlocksDeleted, result.NewBlocksCreated)
	}
}

func (b *BucketStore) RunCompactionWithConfig(config CompactConfig) {
	result := b.runCompactionCycle(config)
	if len(result.Errors) > 0 {
		log.Printf("Compaction completed with %d errors. Blocks compacted: %d, deleted: %d, created: %d",
			len(result.Errors), result.BlocksCompacted, result.BlocksDeleted, result.NewBlocksCreated)
		for _, err := range result.Errors {
			log.Printf("Compaction error: %v", err)
		}
	} else {
		log.Printf("Compaction completed successfully. Blocks compacted: %d, deleted: %d, created: %d",
			result.BlocksCompacted, result.BlocksDeleted, result.NewBlocksCreated)
	}
}

func (b *BucketStore) runCompactionCycle(config CompactConfig) CompactionResult {
	result := CompactionResult{}

	blocks, err := b.listAndCategorizeBlocks()
	if err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("failed to list and categorize blocks: %w", err))
		return result
	}

	serviceBlocks := b.groupBlocksByService(blocks)

	for service, blocks := range serviceBlocks {
		if len(blocks) == 0 {
			continue
		}

		// Compact Level 0 to Level 1
		level0Result := b.compactLevel0ToLevel1(service, blocks, config)
		result.BlocksCompacted += level0Result.BlocksCompacted
		result.NewBlocksCreated += level0Result.NewBlocksCreated
		result.Errors = append(result.Errors, level0Result.Errors...)

		// Compact Level 1 to Level 2
		level1Result := b.compactLevel1ToLevel2(service, blocks, config)
		result.BlocksCompacted += level1Result.BlocksCompacted
		result.NewBlocksCreated += level1Result.NewBlocksCreated
		result.Errors = append(result.Errors, level1Result.Errors...)

		// Clean up old blocks (with grace period and max lifetime)
		cleanupResult := b.cleanUpOldBlocks(blocks, config)
		result.BlocksDeleted += cleanupResult.BlocksDeleted
		result.Errors = append(result.Errors, cleanupResult.Errors...)

		log.Printf("Completed compaction for service: %s", service)
	}

	return result
}

func (b *BucketStore) listAndCategorizeBlocks() ([]BlockInfo, error) {
	var blocks []BlockInfo

	objectCh := b.client.ListObjects(b.ctx, b.config.Bucket, minio.ListObjectsOptions{
		Recursive: true,
	})

	for object := range objectCh {
		if object.Err != nil {
			return nil, object.Err
		}

		if strings.HasSuffix(object.Key, "meta.json") {
			continue
		}

		blockInfo, err := b.parseBlockInfo(object.Key, object.LastModified)
		if err != nil {
			log.Printf("Warning: failed to parse block info for %s: %v", object.Key, err)
			continue // Skip invalid blocks instead of failing entirely
		}

		blocks = append(blocks, blockInfo)
	}

	return blocks, nil
}

// our object key format is: "startTime-endTime/service_LEVEL.pb"
func (b *BucketStore) parseBlockInfo(objectKey string, lastModified time.Time) (BlockInfo, error) {
	parts := strings.Split(objectKey, "/")
	if len(parts) != 2 {
		return BlockInfo{}, errors.New("invalid object key format")
	}

	timeRange := parts[0]
	serviceAndLevel := parts[1]

	timeParts := strings.Split(timeRange, "-")
	if len(timeParts) != 2 {
		return BlockInfo{}, errors.New("invalid time range format")
	}

	minTs, err := strconv.ParseInt(timeParts[0], 10, 64)
	if err != nil {
		return BlockInfo{}, errors.New("invalid min timestamp format")
	}

	maxTs, err := strconv.ParseInt(timeParts[1], 10, 64)
	if err != nil {
		return BlockInfo{}, errors.New("invalid max timestamp format")
	}

	if minTs >= maxTs {
		return BlockInfo{}, errors.New("invalid time range: min timestamp must be less than max timestamp")
	}

	if strings.HasSuffix(serviceAndLevel, ".pb") {
		serviceAndLevel = strings.TrimSuffix(serviceAndLevel, ".pb")
	} else {
		return BlockInfo{}, errors.New("invalid service and level format")
	}

	serviceParts := strings.Split(serviceAndLevel, "_")
	if len(serviceParts) != 2 {
		return BlockInfo{}, errors.New("invalid service and level format")
	}

	service := serviceParts[0]
	levelStr := serviceParts[1]
	level, err := strconv.Atoi(levelStr)
	if err != nil || level < 0 || level > 2 {
		return BlockInfo{}, errors.New("invalid level format, must be 0, 1, or 2")
	}

	// Detect if block was created by compaction (higher levels are typically compacted)
	isCompacted := level > 0

	return BlockInfo{
		ObjectKey:    objectKey,
		Service:      service,
		MinTs:        minTs,
		MaxTs:        maxTs,
		Level:        level,
		LastModified: lastModified,
		IsCompacted:  isCompacted,
		CreatedAt:    lastModified, // more of an "Approximation", track original time of creation
	}, nil
}

func (b *BucketStore) groupBlocksByService(blocks []BlockInfo) map[string][]BlockInfo {
	serviceBlocks := make(map[string][]BlockInfo)

	for _, block := range blocks {
		serviceBlocks[block.Service] = append(serviceBlocks[block.Service], block)
	}

	for service := range serviceBlocks {
		sort.Slice(serviceBlocks[service], func(i, j int) bool {
			if serviceBlocks[service][i].MinTs != serviceBlocks[service][j].MinTs {
				return serviceBlocks[service][i].MinTs < serviceBlocks[service][j].MinTs
			}
			// If MinTs is the same, sort by MaxTs
			return serviceBlocks[service][i].MaxTs < serviceBlocks[service][j].MaxTs
		})
	}

	return serviceBlocks
}

func (b *BucketStore) compactLevel0ToLevel1(service string, blocks []BlockInfo, config CompactConfig) CompactionResult {
	result := CompactionResult{}
	cutOffTime := time.Now().Add(-config.Level0Retention)

	var candidateBlocks []BlockInfo

	for _, block := range blocks {
		if block.Level == 0 && block.LastModified.Before(cutOffTime) {
			candidateBlocks = append(candidateBlocks, block)
		}
	}

	if len(candidateBlocks) == 0 {
		return result
	}

	sort.Slice(candidateBlocks, func(i, j int) bool {
		return candidateBlocks[i].MinTs < candidateBlocks[j].MinTs
	})

	// Group into time-contiguous batches
	batches := b.groupBlocksIntoBatches(candidateBlocks, config.Level1BatchSize, config.MaxTimeGapBetweenBlocks)

	for _, batch := range batches {
		if !config.AllowIncompleteBatches && len(batch) < config.Level1BatchSize {
			log.Printf("Skipping incomplete batch of %d blocks for service %s (need %d)",
				len(batch), service, config.Level1BatchSize)
			continue
		}

		if err := b.compactBlocks(batch, 1, config); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("failed to compact blocks for service %s: %w", service, err))
			continue
		}

		result.BlocksCompacted += len(batch)
		result.NewBlocksCreated++
		log.Printf("Compacted %d blocks into 12h block for service %s", len(batch), service)
	}

	return result
}

// compactLevel1ToLevel2 compacts 12h blocks into 24h blocks
func (b *BucketStore) compactLevel1ToLevel2(service string, blocks []BlockInfo, config CompactConfig) CompactionResult {
	result := CompactionResult{}
	cutOffTime := time.Now().Add(-config.Level1Retention)

	var candidateBlocks []BlockInfo

	for _, block := range blocks {
		if block.Level == 1 && block.LastModified.Before(cutOffTime) {
			candidateBlocks = append(candidateBlocks, block)
		}
	}

	if len(candidateBlocks) == 0 {
		return result
	}

	sort.Slice(candidateBlocks, func(i, j int) bool {
		return candidateBlocks[i].MinTs < candidateBlocks[j].MinTs
	})

	// Group into time-contiguous batches
	batches := b.groupBlocksIntoBatches(candidateBlocks, config.Level2BatchSize, config.MaxTimeGapBetweenBlocks)

	for _, batch := range batches {
		if !config.AllowIncompleteBatches && len(batch) < config.Level2BatchSize {
			log.Printf("Skipping incomplete batch of %d blocks for service %s (need %d)",
				len(batch), service, config.Level2BatchSize)
			continue
		}

		if err := b.compactBlocks(batch, 2, config); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("failed to compact blocks for service %s: %w", service, err))
			continue
		}

		result.BlocksCompacted += len(batch)
		result.NewBlocksCreated++
		log.Printf("Compacted %d blocks into 24h block for service %s", len(batch), service)
	}

	return result
}

// groupBlocksIntoBatches groups blocks into time-contiguous batches
func (b *BucketStore) groupBlocksIntoBatches(blocks []BlockInfo, batchSize int, maxTimeGap int64) [][]BlockInfo {
	if len(blocks) == 0 {
		return nil
	}

	var batches [][]BlockInfo
	var currentBatch []BlockInfo

	for _, block := range blocks {
		// Start new batch if current is empty
		if len(currentBatch) == 0 {
			currentBatch = append(currentBatch, block)
			continue
		}

		lastBlock := currentBatch[len(currentBatch)-1]

		// Check if this block can be added to current batch
		timeGap := block.MinTs - lastBlock.MaxTs

		if len(currentBatch) >= batchSize || timeGap > maxTimeGap {
			// Start new batch
			batches = append(batches, currentBatch)
			currentBatch = []BlockInfo{block}
		} else {
			// Add to current batch
			currentBatch = append(currentBatch, block)
		}
	}

	// Add the last batch if it has blocks
	if len(currentBatch) > 0 {
		batches = append(batches, currentBatch)
	}

	return batches
}

func (b *BucketStore) compactBlocks(blocks []BlockInfo, targetLevel int, config CompactConfig) error {
	var series []*logapi.Series
	var minT, maxT int64

	for _, block := range blocks {
		currentSeries, err := b.loadSeriesFromBlock(block.ObjectKey)
		if err != nil {
			return fmt.Errorf("failed to load series from block %s: %w", block.ObjectKey, err)
		}

		series = append(series, currentSeries...)

		if minT == 0 || block.MinTs < minT {
			minT = block.MinTs
		}
		if maxT == 0 || block.MaxTs > maxT {
			maxT = block.MaxTs
		}
	}

	if len(series) == 0 {
		return errors.New("no series found in blocks to compact")
	}

	// Sort series by timestamp
	sort.Slice(series, func(i, j int) bool {
		return series[i].Entry.Timestamp < series[j].Entry.Timestamp
	})

	newBlockKey := b.generateCompactedBlockKey(blocks, targetLevel, minT, maxT)

	if err := b.writeCompactedBlock(newBlockKey, series); err != nil {
		return fmt.Errorf("failed to write compacted block %s: %w", newBlockKey, err)
	}

	log.Printf("Successfully created compacted block %s from %d source blocks", newBlockKey, len(blocks))

	// !!! We don't delete the source blocks here anymore.
	// The cleanUpOldBlocks function will handle deletion based on:
	// 1. Grace period after compaction
	// 2. Maximum block lifetime
	// 3. Retention policies

	return nil
}

func (b *BucketStore) cleanUpOldBlocks(blocks []BlockInfo, config CompactConfig) CompactionResult {
	result := CompactionResult{}
	now := time.Now()

	level0CutOffTime := now.Add(-config.Level0Retention - config.CompactionGracePeriod)
	level1CutOffTime := now.Add(-config.Level1Retention - config.CompactionGracePeriod)
	// level2CutOffTime := now.Add(-config.Level2Retention) L2 is last level of compaction, deleting this will result in data loss
	maxLifetimeCutOff := now.Add(-config.MaxBlockLifetime)

	for _, block := range blocks {
		shouldDelete := false
		reason := ""

		if block.Level == 0 && block.LastModified.Before(maxLifetimeCutOff) {
			shouldDelete = true
			reason = fmt.Sprintf("exceeded maximum lifetime of %v", config.MaxBlockLifetime)
		} else if block.Level == 0 && block.LastModified.Before(level0CutOffTime) {
			// L0 blocks: Only delete if max lifetime exceeded
			// We don't delete L0 blocks based on retention period because:
			// 1. They are original data
			// 2. Compaction might have failed
			// 3. We can't verify if they were successfully compacted without additional tracking
			// TODO: Implement compaction success tracking to safely delete L0 blocks
		} else if block.Level == 1 && block.LastModified.Before(level1CutOffTime) {
			// Level 1 blocks that are old enough to have been compacted + grace period
			shouldDelete = true
			reason = fmt.Sprintf("level 1 block exceeded retention + grace period (%v + %v)",
				config.Level1Retention, config.CompactionGracePeriod)
		}

		if shouldDelete {
			if err := b.client.RemoveObject(b.ctx, b.config.Bucket, block.ObjectKey, minio.RemoveObjectOptions{}); err != nil {
				result.Errors = append(result.Errors, fmt.Errorf("failed to remove old block %s: %w", block.ObjectKey, err))
				log.Printf("Failed to remove old block %s: %v", block.ObjectKey, err)
			} else {
				result.BlocksDeleted++
				log.Printf("Removed old block %s (%s)", block.ObjectKey, reason)
			}
		}
	}

	return result
}

func (b *BucketStore) generateCompactedBlockKey(blocks []BlockInfo, targetLevel int, minT, maxT int64) string {
	service := blocks[0].Service
	levelSuffix := fmt.Sprintf("_%d", targetLevel)
	return fmt.Sprintf("%d-%d/%s%s.pb", minT, maxT, service, levelSuffix)
}

func (b *BucketStore) writeCompactedBlock(key string, series []*logapi.Series) error {
	batch := &logapi.SeriesBatch{
		Entries: series,
	}

	return b.uploadLogsToStorage(batch, key)
}
