package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"github.com/Saumya40-codes/LogsGO/pkg"
	"github.com/Saumya40-codes/LogsGO/pkg/logsgoql"
	"github.com/Saumya40-codes/LogsGO/pkg/metrics"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

type BucketStoreConfig struct {
	Provider            string `yaml:"provider"`
	Endpoint            string `yaml:"endpoint"`
	Bucket              string `yaml:"bucket"`
	Region              string `yaml:"region"`
	AccessKey           string `yaml:"access_key"`
	SecretKey           string `yaml:"secret_key"`
	UseSSL              bool   `yaml:"use_ssl"`
	CreateBucketOnEmpty bool   `yaml:"create_bucket_on_empty"`
}

type Config struct {
	RemoteStore BucketStoreConfig `yaml:"remote_store"`
}

type BucketStore struct {
	client  *minio.Client
	mu      sync.Mutex
	config  BucketStoreConfig
	ctx     context.Context
	index   *ShardedLogIndex // shared log index
	metrics *metrics.Metrics
}

func NewBucketStore(ctx context.Context, path string, storeConfig string, index *ShardedLogIndex, metrics *metrics.Metrics, compactDuration string, compactConfig string) (*BucketStore, error) {
	var configData []byte
	var err error

	if path != "" {
		configData, err = os.ReadFile(path)
		if err != nil {
			return nil, err
		}
	} else if storeConfig != "" {
		configData = []byte(storeConfig)
	} else {
		return nil, fmt.Errorf("either path or storeConfig must be provided")
	}

	var BucketCfg Config
	err = yaml.Unmarshal(configData, &BucketCfg)
	if err != nil {
		return nil, err
	}

	if err = validateConfiguration(BucketCfg.RemoteStore); err != nil {
		return nil, err
	}

	store := &BucketStore{
		config:  BucketCfg.RemoteStore,
		mu:      sync.Mutex{},
		ctx:     ctx,
		index:   index,
		metrics: metrics,
	}

	if err = store.InitClient(); err != nil {
		return nil, err
	}

	// start the compaction routine
	log.Println("Starting compaction routine for bucket store")
	if compactConfig == "" {
		go store.runCompact(pkg.GetTimeDuration(compactDuration))
	} else {
		var compactCfg CompactConfig
		err = yaml.Unmarshal([]byte(compactConfig), &compactCfg)
		if err != nil {
			return nil, err
		}

		go store.runCompactWithConfig(pkg.GetTimeDuration(compactDuration), compactCfg)
	}

	return store, nil
}

func (b *BucketStore) InitClient() error {
	client, err := minio.New(b.config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(b.config.AccessKey, b.config.SecretKey, ""),
		Secure: b.config.UseSSL,
	})
	if err != nil {
		return err
	}

	b.client = client

	if b.config.CreateBucketOnEmpty {
		err = client.MakeBucket(b.ctx, b.config.Bucket, minio.MakeBucketOptions{
			Region: b.config.Region, // should not be minio thing, compatible for othet S3s
		})
		if err != nil {
			exists, errBucketExists := client.BucketExists(b.ctx, b.config.Bucket)
			if errBucketExists == nil && exists {
				log.Printf("We already own %s\n", b.config.Bucket)
			} else {
				return err
			}
		}
	}

	// create meta.json file if it doesn't exist
	if _, err := b.client.StatObject(b.ctx, b.config.Bucket, "meta.json", minio.StatObjectOptions{}); err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			metaData := &Labels{
				Services: make(map[string]int),
				Levels:   make(map[string]int),
			}
			merr := b.UpdateMetaData(metaData)
			if merr != nil {
				return fmt.Errorf("failed to create meta.json in bucket: %w", merr)
			}
			log.Println("Created meta.json in bucket")
		} else {
			return fmt.Errorf("failed to check meta.json in bucket: %w", err)
		}
	}
	return nil
}

func (b *BucketStore) Insert(logs []*logapi.LogEntry, series map[LogKey]map[int64]CounterValue) error {
	// now here have a dedicated file kinda thing for each entry won't make sense.
	// we will create a chunks of 2h worth of data

	timer := prometheus.NewTimer(b.metrics.IngestionDuration.WithLabelValues("bucket"))
	defer timer.ObserveDuration()

	if len(logs) == 0 {
		log.Println("No log to insert, skipping this cycle")
		return nil
	}

	slices.SortFunc(logs, func(a, b *logapi.LogEntry) int {
		if a.Timestamp < b.Timestamp {
			return -1
		}
		if a.Timestamp > b.Timestamp {
			return 1
		}
		return 0
	})

	baseTimeStamp := logs[0].Timestamp
	nextTimeStamp := getNextTimeStamp(baseTimeStamp, 2*time.Hour)

	batches := &logapi.SeriesBatch{
		Entries: make([]*logapi.Series, 0),
	}
	// newer blocks will always be at level 0 retention
	key := fmt.Sprintf("%d-%d/%s_0.pb", baseTimeStamp, nextTimeStamp, logs[0].Service)

	// get metadata
	metaData, err := b.GetMetaData()
	if err != nil {
		return fmt.Errorf("failed to get metadata: %w", err)
	}

	for _, log := range logs {
		metaData.Services[log.Service]++
		metaData.Levels[log.Level]++

		if log.Timestamp > nextTimeStamp {
			// our logs are sorted, so we can call it end here for one batch
			err := b.uploadLogsToStorage(batches, key)
			if err != nil {
				return err
			}
			batches.Entries = batches.Entries[:0]

			baseTimeStamp = log.Timestamp
			nextTimeStamp = getNextTimeStamp(baseTimeStamp, 2*time.Hour)

			// newer blocks will always be at level 0 retention
			key = fmt.Sprintf("%d-%d/%s_0.pb", baseTimeStamp, nextTimeStamp, log.Service)
		}

		logkey := LogKey{Service: log.Service, Level: log.Level, Message: log.Message}
		logSeries, ok := series[logkey]
		if !ok {
			return fmt.Errorf("logKey %v not found in series", logkey)
		}

		entry, ok := logSeries[log.Timestamp]
		if !ok {
			return fmt.Errorf("timestamp %v not found in logKey series", log.Timestamp)
		}
		series := &logapi.Series{
			Entry: log,
			Count: uint64(entry.value),
		}
		batches.Entries = append(batches.Entries, series)
	}

	// Update metadata
	if err := b.UpdateMetaData(metaData); err != nil {
		return fmt.Errorf("failed to update metadata: %w", err)
	}

	// Upload last batch if exists
	if len(batches.Entries) > 0 {
		if err := b.uploadLogsToStorage(batches, key); err != nil {
			return err
		}
	}

	b.metrics.LogsIngested.WithLabelValues("bucket").Add(float64(len(logs)))
	b.metrics.CurrentLogsIngested.WithLabelValues("bucket").Add(float64(len(logs))) // In all ideal sense, this should be counter, as there won't be flush calls in bucket store
	// TODO: During compaction(when it will implemented), we should update values to this guage

	return nil
}

func (b *BucketStore) uploadLogsToStorage(batch *logapi.SeriesBatch, objectName string) error {
	b.metrics.BucketCalls.Inc()
	data, err := proto.Marshal(batch)
	if err != nil {
		return fmt.Errorf("failed to marshal protobuf: %w", err)
	}

	reader := bytes.NewReader(data)
	_, err = b.client.PutObject(
		b.ctx,
		b.config.Bucket,
		objectName,
		reader,
		int64(len(data)),
		minio.PutObjectOptions{
			ContentType: "application/octet-stream",
		},
	)
	if err != nil {
		return fmt.Errorf("failed to upload to S3 storage: %w", err)
	}

	log.Println("Uploaded log to S3")
	return nil
}

func (b *BucketStore) Close() error {
	// passed ctx should do the job
	return nil
}

func (b *BucketStore) Flush() error {
	return nil // nothing next to bucket store
}

// LabelValues returns the unique label values from the local store.
func (b *BucketStore) LabelValues(labels *Labels) error {
	if labels == nil {
		return fmt.Errorf("labels cannot be nil")
	}

	metaData, err := b.GetMetaData()
	if err != nil {
		return fmt.Errorf("failed to get metadata: %w", err)
	}
	for service := range metaData.Services {
		if _, exists := labels.Services[service]; !exists {
			labels.Services[service] = 0
		}
	}
	for level := range metaData.Levels {
		if _, exists := labels.Levels[level]; !exists {
			labels.Levels[level] = 0
		}
	}

	return nil
}

func (b *BucketStore) QueryInstant(cfg *logsgoql.InstantQueryConfig) ([]InstantQueryResponse, error) {
	cfg.Cache.BucketOnce.Do(func() {
		// Fast-path: if query is service-scoped, try index to find compacted blocks covering the range
		if cfg.Filter.Service != "" {
			minT := cfg.Ts - cfg.Lookback
			maxT := cfg.Ts
			if entries, err := b.findBestCompactedBlocks(cfg.Filter.Service, minT, maxT); err == nil && entries != nil {
				var all []*logapi.Series
				ok := true
				for _, e := range entries {
					s, err := b.loadSeriesFromBlock(e.Key)
					if err != nil {
						ok = false
						break
					}
					all = append(all, s...)
				}
				if ok {
					cfg.Cache.BucketData = all
					slices.SortFunc(cfg.Cache.BucketData, func(a, b *logapi.Series) int {
						if a.Entry.Timestamp < b.Entry.Timestamp {
							return 1
						}
						if a.Entry.Timestamp > b.Entry.Timestamp {
							return -1
						}
						return 0
					})
					// fast-path succeeded
					return
				}
			}
			// fallthrough to full fetch on error or no entries
		}

		b.fetchLogs(&cfg.Cache.BucketData)
		slices.SortFunc(cfg.Cache.BucketData, func(a, b *logapi.Series) int {
			if a.Entry.Timestamp < b.Entry.Timestamp {
				return 1
			}
			if a.Entry.Timestamp > b.Entry.Timestamp {
				return -1
			}
			return 0
		})
	})

	var result []InstantQueryResponse

	if cfg.Filter.LHS == nil && cfg.Filter.RHS == nil {
		for _, log := range cfg.Cache.BucketData {
			// break this log if it falls outside lookback period, we sort above so this is fine
			if cfg.Ts-cfg.Lookback > log.Entry.Timestamp {
				break
			}
			if (cfg.Filter.Level == "" || log.Entry.Level == cfg.Filter.Level) && (cfg.Filter.Service == "" || log.Entry.Service == cfg.Filter.Service) {
				result = append(result, InstantQueryResponse{
					Service:   log.Entry.Service,
					Level:     log.Entry.Level,
					Message:   log.Entry.Message,
					Count:     log.Count,
					TimeStamp: log.Entry.Timestamp,
				})
			}
		}
	} else {
		lhsResults := make([]InstantQueryResponse, 0)
		var err error
		if cfg.Filter.LHS != nil {
			lhsCfg := *cfg
			lhsCfg.Filter = *cfg.Filter.LHS
			lhsResults, err = b.QueryInstant(&lhsCfg)
			if err != nil {
				return nil, fmt.Errorf("failed to query LHS: %w", err)
			}
		}

		rhsResults := make([]InstantQueryResponse, 0)
		if cfg.Filter.RHS != nil {
			rhsCfg := *cfg
			rhsCfg.Filter = *cfg.Filter.RHS
			rhsResults, err = b.QueryInstant(&rhsCfg)
			if err != nil {
				return nil, fmt.Errorf("failed to query RHS: %w", err)
			}
		}

		if cfg.Filter.Or {
			result = append(result, lhsResults...)
			result = append(result, rhsResults...)
		} else {
			rhsMap := make(map[string]InstantQueryResponse)
			for _, r := range rhsResults {
				key := r.Service + "|" + r.Level + "|" + strconv.FormatInt(r.TimeStamp, 10)
				rhsMap[key] = r
			}
			for _, l := range lhsResults {
				key := l.Service + "|" + l.Level + "|" + strconv.FormatInt(l.TimeStamp, 10)
				if _, exists := rhsMap[key]; exists {
					result = append(result, l)
				}
			}
		}
	}
	return result, nil
}

func (b *BucketStore) QueryRange(cfg *logsgoql.RangeQueryConfig) ([]QueryResponse, error) {
	return nil, nil
}

func (b *BucketStore) fetchLogs(logs *[]*logapi.Series) error {
	b.metrics.BucketCalls.Inc()
	// var results []*logapi.LogEntry
	ctx := b.ctx

	objectCh := b.client.ListObjects(ctx, b.config.Bucket, minio.ListObjectsOptions{
		Recursive: true,
	})

	for object := range objectCh {
		if object.Err != nil {
			return object.Err
		}

		// TODO: utilize the key name we set to fetch stuffs
		obj, err := b.client.GetObject(ctx, b.config.Bucket, object.Key, minio.GetObjectOptions{})
		if err != nil {
			return err
		}

		data, err := io.ReadAll(obj)
		if err != nil {
			return err
		}

		batch := &logapi.SeriesBatch{}
		if err := proto.Unmarshal(data, batch); err != nil {
			return err
		}

		if len(batch.Entries) > 0 {
			*logs = append(*logs, batch.Entries...)
		}
	}
	return nil
}

func (b *BucketStore) loadSeriesFromBlock(objectKey string) ([]*logapi.Series, error) {
	b.metrics.BucketCalls.Inc()
	ctx := b.ctx

	obj, err := b.client.GetObject(ctx, b.config.Bucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get object %s: %w", objectKey, err)
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to read object %s: %w", objectKey, err)
	}

	batch := &logapi.SeriesBatch{}
	if err := proto.Unmarshal(data, batch); err != nil {
		return nil, fmt.Errorf("failed to unmarshal data from %s: %w", objectKey, err)
	}

	return batch.Entries, nil
}

func (b *BucketStore) GetMetaData() (*Labels, error) {
	b.metrics.BucketCalls.Inc()
	labels := &Labels{
		Services: make(map[string]int),
		Levels:   make(map[string]int),
	}

	metaFile := "meta.json"
	obj, err := b.client.GetObject(b.ctx, b.config.Bucket, metaFile, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata file: %w", err)
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata file: %w", err)
	}

	if err := json.Unmarshal(data, labels); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return labels, nil
}

func (b *BucketStore) UpdateMetaData(labels *Labels) error {
	b.metrics.BucketCalls.Inc()
	metaFile := "meta.json"
	data, err := json.Marshal(labels)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	reader := bytes.NewReader(data)
	_, err = b.client.PutObject(
		b.ctx,
		b.config.Bucket,
		metaFile,
		reader,
		int64(len(data)),
		minio.PutObjectOptions{
			ContentType: "application/json",
		},
	)
	if err != nil {
		return fmt.Errorf("failed to update metadata in S3 storage: %w", err)
	}

	log.Println("Updated metadata in S3")
	return nil
}

func (b *BucketStore) runCompact(duration time.Duration) {
	ticker := time.NewTicker(duration)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.RunCompaction()
		case <-b.ctx.Done():
			return
		}
	}
}

func (b *BucketStore) runCompactWithConfig(duration time.Duration, config CompactConfig) {
	ticker := time.NewTicker(duration)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.RunCompactionWithConfig(config)
		case <-b.ctx.Done():
			return
		}
	}
}

func (b *BucketStore) getServiceIndex(service string) (*ServiceIndex, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.metrics.BucketCalls.Inc()
	objectName := fmt.Sprintf("index/%s.json", service)

	if _, err := b.client.StatObject(b.ctx, b.config.Bucket, objectName, minio.StatObjectOptions{}); err != nil {
		return &ServiceIndex{Entries: []IndexEntry{}}, nil
	}

	obj, err := b.client.GetObject(b.ctx, b.config.Bucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, err
	}

	idx := &ServiceIndex{}
	if len(data) == 0 {
		return idx, nil
	}

	if err := json.Unmarshal(data, idx); err != nil {
		return nil, err
	}
	return idx, nil
}

func (b *BucketStore) putServiceIndex(service string, idx *ServiceIndex) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.metrics.BucketCalls.Inc()
	objectName := fmt.Sprintf("index/%s.json", service)

	data, err := json.Marshal(idx)
	if err != nil {
		return err
	}

	reader := bytes.NewReader(data)
	_, err = b.client.PutObject(b.ctx, b.config.Bucket, objectName, reader, int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/json"})

	if err != nil {
		fmt.Printf("Failed to put service index for %s: %v\n", service, err)
	}

	return err
}
