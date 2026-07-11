package store

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/Saumya40-codes/LogsGO/pkg/metrics"
	"github.com/minio/minio-go/v7"
)

type s3ReaderAt struct {
	client  *minio.Client
	ctx     context.Context
	bucket  string
	key     string
	size    int64
	metrics *metrics.Metrics

	rangeGets atomic.Int64
	bytesRead atomic.Int64
}

func newS3ReaderAt(ctx context.Context, client *minio.Client, bucket, key string, m *metrics.Metrics) (*s3ReaderAt, error) {
	if m != nil {
		m.BucketCalls.Inc()
	}
	info, err := client.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("stat object %s: %w", key, err)
	}
	return &s3ReaderAt{
		client:  client,
		ctx:     ctx,
		bucket:  bucket,
		key:     key,
		size:    info.Size,
		metrics: m,
	}, nil
}

func (r *s3ReaderAt) Size() int64 { return r.size }

func (r *s3ReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, fmt.Errorf("negative offset %d", off)
	}
	if off >= r.size {
		return 0, io.EOF
	}

	end := off + int64(len(p)) - 1
	if end >= r.size {
		end = r.size - 1
	}
	want := int(end - off + 1)

	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(off, end); err != nil {
		return 0, fmt.Errorf("set range [%d,%d]: %w", off, end, err)
	}

	if r.metrics != nil {
		r.metrics.BucketCalls.Inc()
	}
	r.rangeGets.Add(1)

	obj, err := r.client.GetObject(r.ctx, r.bucket, r.key, opts)
	if err != nil {
		return 0, fmt.Errorf("range get %s [%d,%d]: %w", r.key, off, end, err)
	}
	defer obj.Close()

	n, err := io.ReadFull(obj, p[:want])
	if n > 0 {
		r.bytesRead.Add(int64(n))
	}
	if err == io.ErrUnexpectedEOF {
		if n == 0 {
			return 0, io.EOF
		}
		return n, io.EOF
	}
	if err != nil && err != io.EOF {
		return n, fmt.Errorf("read range %s [%d,%d]: %w", r.key, off, end, err)
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
