package store

import (
	"bytes"
	"context"
	"fmt"
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

func NewBucketStore(ctx context.Context, path string) (*BucketStore, error) {
	configData, err := os.ReadFile(path)
	if err != nil {
		return nil, err
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

func (b *BucketStore) Insert(logs []*logapi.LogEntry) error {
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

	batches := make(map[string][]*logapi.LogEntry)

	for _, log := range logs {
		if log.Timestamp > nextTimeStamp {
			// our logs are sorted, so we can call it end here for one batch
			baseTimeStamp = log.Timestamp
			nextTimeStamp = getNextTimeStamp(baseTimeStamp, 2*time.Hour)
		}

		key := fmt.Sprintf("%d-%d/%s.pb", baseTimeStamp, nextTimeStamp, log.Service)
		batches[key] = append(batches[key], log)
	}

	for key, logs := range batches {
		batch := &logapi.LogBatch{
			Entries: logs,
		}

		data, err := proto.Marshal(batch)
		if err != nil {
			return fmt.Errorf("failed to marshal protobuf: %w", err)
		}

		reader := bytes.NewReader(data)
		_, err = b.client.PutObject(
			b.ctx,
			b.config.Bucket,
			key,
			reader,
			int64(len(data)),
			minio.PutObjectOptions{
				ContentType: "application/octet-stream",
			},
		)

		if err != nil {
			return fmt.Errorf("failed to upload to S3 storage: %w", err)
		}
	}
	return nil
}
