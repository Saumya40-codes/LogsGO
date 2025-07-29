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

	// Downsampling intervals
	Level1Interval int64 `yaml:"level1_interval"` // Original interval (e.g., 1s)
	Level2Interval int64 `yaml:"level2_interval"` // First downsampling (e.g., 1m)
	Level3Interval int64 `yaml:"level3_interval"` // Second downsampling (e.g., 5m)

	// Compaction batch sizes
	Level1BatchSize int `yaml:"level1_batch_size"` // Number of 2h blocks to merge into 12h
	Level2BatchSize int `yaml:"level2_batch_size"` // Number of 12h blocks to merge into 24h
}

type BlockInfo struct {
	ObjectKey    string
	Service      string
	MinTs        int64
	MaxTs        int64
	Level        int // 0 = 2h, 1 = 12h, 2 = 24h
	LastModified time.Time
}

// Run Compaction is a method that triggers the compaction process for the BucketStore.
// For now its non-configurable
func (b *BucketStore) RunCompaction() {
	config := CompactConfig{
		Level0Retention: 24 * time.Hour,      // 2h blocks kept for 24h
		Level1Retention: 7 * 24 * time.Hour,  // 12h blocks kept for 7 days
		Level2Retention: 30 * 24 * time.Hour, // 24h blocks kept for 30 days
		Level1BatchSize: 6,                   // 6 blocks of 2h to make 12h
		Level2BatchSize: 2,                   // 2 blocks of 12h to make 24h
	}

	if err := b.runCompactionCycle(config); err != nil {
		log.Printf("Compaction failed: %v", err)
	}
}

func (b *BucketStore) RunCompactionWithConfig(config CompactConfig) {
	if err := b.runCompactionCycle(config); err != nil {
		log.Printf("Compaction failed: %v", err)
	}
}

func (b *BucketStore) runCompactionCycle(config CompactConfig) error {

	blocks, err := b.listAndCategorizeBlocks()
	if err != nil {
		return fmt.Errorf("failed to list and categorize blocks: %w", err)
	}

	serviceBlocks := b.groupBlocksByService(blocks)

	for service, blocks := range serviceBlocks {
		if len(blocks) == 0 {
			continue
		}

		if err := b.compactLevel0ToLevel1(service, blocks, config); err != nil {
			return fmt.Errorf("failed to compact Level 0 blocks for service %s: %w", service, err)
		}

		if err := b.compactLevel1ToLevel2(service, blocks, config); err != nil {
			return fmt.Errorf("failed to compact Level 1 blocks for service %s: %w", service, err)
		}

		if err := b.cleanUpOldBlocks(blocks, config); err != nil {
			return fmt.Errorf("failed to clean up old blocks for service %s: %w", service, err)
		}

		log.Printf("Completed compaction for service: %s", service)
	}

	return nil
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
			return nil, err
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

	return BlockInfo{
		ObjectKey:    objectKey,
		Service:      service,
		MinTs:        minTs,
		MaxTs:        maxTs,
		Level:        level,
		LastModified: lastModified,
	}, nil
}

func (b *BucketStore) groupBlocksByService(blocks []BlockInfo) map[string][]BlockInfo {
	serviceBlocks := make(map[string][]BlockInfo)

	for _, block := range blocks {
		serviceBlocks[block.Service] = append(serviceBlocks[block.Service], block)
	}

	for service := range serviceBlocks {
		sort.Slice(serviceBlocks[service], func(i, j int) bool {
			return serviceBlocks[service][i].MinTs < serviceBlocks[service][j].MaxTs
		})
	}

	return serviceBlocks
}

func (b *BucketStore) compactLevel0ToLevel1(service string, blocks []BlockInfo, config CompactConfig) error {
	cutOffTime := time.Now().Add(-config.Level0Retention)

	var candidateBlocks []BlockInfo

	for _, block := range blocks {
		if block.Level == 0 && block.LastModified.Before(cutOffTime) {
			candidateBlocks = append(candidateBlocks, block)
		}
	}

	// gather into batches of config.Level1BatchSize

	for i := 0; i < len(candidateBlocks); i += config.Level1BatchSize {
		end := min(i+config.Level1BatchSize, len(candidateBlocks))
		batch := candidateBlocks[i:end]
		if len(batch) < config.Level1BatchSize {
			continue // we'll skip incomplete batches for now
		}

		if err := b.compactBlocks(batch, 1); err != nil {
			return fmt.Errorf("failed to compact blocks for service %s: %w", service, err)
		}
		log.Printf("Compacted %d blocks into 12h block for service %s", len(batch), service)
	}

	return nil
}

// compactLevel1ToLevel2 compacts 12h blocks into 24h blocks
func (b *BucketStore) compactLevel1ToLevel2(service string, blocks []BlockInfo, config CompactConfig) error {
	cutOffTime := time.Now().Add(-config.Level1Retention)

	var candidateBlocks []BlockInfo

	for _, block := range blocks {
		if block.Level == 1 && block.LastModified.Before(cutOffTime) {
			candidateBlocks = append(candidateBlocks, block)
		}
	}

	for i := 0; i < len(candidateBlocks); i += config.Level2BatchSize {
		end := min(i+config.Level2BatchSize, len(candidateBlocks))

		batch := candidateBlocks[i:end]
		if len(batch) < config.Level2BatchSize {
			continue // we'll skip incomplete batches for now
		}

		if err := b.compactBlocks(batch, 2); err != nil {
			return fmt.Errorf("failed to compact blocks for service %s: %w", service, err)
		}
		log.Printf("Compacted %d blocks into 24h block for service %s", len(batch), service)
	}
	return nil
}

func (b *BucketStore) compactBlocks(blocks []BlockInfo, targetLevel int) error {
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

	sort.Slice(series, func(i, j int) bool {
		return series[i].Entry.Timestamp < series[j].Entry.Timestamp
	})

	newBlockKey := b.generateCompactedBlockKey(blocks, targetLevel, minT, maxT)

	if err := b.writeCompactedBlock(newBlockKey, series); err != nil {
		return fmt.Errorf("failed to write compacted block %s: %w", newBlockKey, err)
	}

	log.Printf("Compacted %d blocks into %s", len(blocks), newBlockKey)
	for _, block := range blocks {
		if err := b.client.RemoveObject(b.ctx, b.config.Bucket, block.ObjectKey, minio.RemoveObjectOptions{}); err != nil {
			log.Printf("Failed to remove old block %s: %v", block.ObjectKey, err)
		} else {
			log.Printf("Removed old block %s after compaction", block.ObjectKey)
		}
	}

	log.Printf("Successfully compacted %d blocks into %s", len(blocks), newBlockKey)
	return nil
}

func (b *BucketStore) cleanUpOldBlocks(blocks []BlockInfo, config CompactConfig) error {
	cutOffTime := time.Now().Add(-config.Level2Retention)

	for _, block := range blocks {
		if block.Level == 2 && block.LastModified.Before(cutOffTime) {
			if err := b.client.RemoveObject(b.ctx, b.config.Bucket, block.ObjectKey, minio.RemoveObjectOptions{}); err != nil {
				log.Printf("Failed to remove old block %s: %v", block.ObjectKey, err)
			} else {
				log.Printf("Removed old block %s after retention period", block.ObjectKey)
			}
		}
	}

	return nil
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
