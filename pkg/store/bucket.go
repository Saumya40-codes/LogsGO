package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"sync"
	"time"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
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
	client *minio.Client
	mu     sync.Mutex
	config BucketStoreConfig
	ctx    context.Context
}

func NewBucketStore(ctx context.Context, path string, storeConfig string) (*BucketStore, error) {
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
		config: BucketCfg.RemoteStore,
		mu:     sync.Mutex{},
		ctx:    ctx,
	}

	if err = store.InitClient(); err != nil {
		return nil, err
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
	return nil
}

func (b *BucketStore) Insert(logs []*logapi.LogEntry, series map[LogKey]map[int64]*CounterValue) error {
	// now here have a dedicated file kinda thing for each entry won't make sense.
	// we will create a chunks of 2h worth of data

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
	key := fmt.Sprintf("%d-%d/%s.pb", baseTimeStamp, nextTimeStamp, logs[0].Service)

	for _, log := range logs {
		if log.Timestamp > nextTimeStamp {
			// our logs are sorted, so we can call it end here for one batch
			err := b.uploadLogsToStorage(batches, key)
			if err != nil {
				return err
			}
			batches.Entries = batches.Entries[:0]

			baseTimeStamp = log.Timestamp
			nextTimeStamp = getNextTimeStamp(baseTimeStamp, 2*time.Hour)

			key = fmt.Sprintf("%d-%d/%s.pb", baseTimeStamp, nextTimeStamp, log.Service)
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

	// Upload last batch if exists
	if len(batches.Entries) > 0 {
		if err := b.uploadLogsToStorage(batches, key); err != nil {
			return err
		}
	}

	return nil
}

func (b *BucketStore) uploadLogsToStorage(batch *logapi.SeriesBatch, objectName string) error {
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
	var logs []*logapi.Series
	err := b.fetchLogs(&logs)
	if err != nil {
		return err
	}

	for _, log := range logs {
		labels.Services[log.Entry.Service] = struct{}{}
		labels.Levels[log.Entry.Level] = struct{}{}
	}

	return nil
}

func (b *BucketStore) Query(filter LogFilter, lookback int64, qTime int64) ([]QueryResponse, error) {
	var logs []*logapi.Series
	b.fetchLogs(&logs)
	var result []QueryResponse

	slices.SortFunc(logs, func(a, b *logapi.Series) int {
		if a.Entry.Timestamp < b.Entry.Timestamp {
			return 1
		}
		if a.Entry.Timestamp > b.Entry.Timestamp {
			return -1
		}
		return 0
	})

	if filter.LHS == nil && filter.RHS == nil {
		for _, log := range logs {
			// Skip this log if it falls outside lookback period
			if qTime-lookback > log.Entry.Timestamp {
				continue
			}
			if (filter.Level == "" || log.Entry.Level == filter.Level) && (filter.Service == "" || log.Entry.Service == filter.Service) {
				result = append(result, QueryResponse{
					Service:   log.Entry.Service,
					Level:     log.Entry.Level,
					Message:   log.Entry.Message,
					Count:     log.Count,
					TimeStamp: log.Entry.Timestamp,
				})
				break
			}
		}
	} else {
		if filter.Or {
			lhsResults, err := b.Query(*filter.LHS, lookback, qTime)
			if err != nil {
				return nil, fmt.Errorf("failed to query LHS: %w", err)
			}
			rhsResults, err := b.Query(*filter.RHS, lookback, qTime)
			if err != nil {
				return nil, fmt.Errorf("failed to query RHS: %w", err)
			}
			result = append(lhsResults, rhsResults...)
		} else {
			lhsResults, err := b.Query(*filter.LHS, lookback, qTime)
			if err != nil {
				return nil, fmt.Errorf("failed to query LHS: %w", err)
			}
			rhsResults, err := b.Query(*filter.RHS, lookback, qTime)
			if err != nil {
				return nil, fmt.Errorf("failed to query RHS: %w", err)
			}
			for _, lhsLog := range lhsResults {
				for _, rhsLog := range rhsResults {
					if lhsLog.Service == rhsLog.Service && lhsLog.Level == rhsLog.Level && lhsLog.TimeStamp == rhsLog.TimeStamp {
						result = append(result, lhsLog)
					}
				}
			}
		}
	}
	return result, nil
}

func (b *BucketStore) fetchLogs(logs *[]*logapi.Series) error {
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
