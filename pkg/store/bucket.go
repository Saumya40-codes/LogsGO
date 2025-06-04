package store

import (
	"context"
	"log"
	"os"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
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
