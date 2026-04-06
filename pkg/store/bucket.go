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
	"sort"
	"strconv"
	"strings"
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
		s := &logapi.Series{
			Entry: log,
			Count: uint64(entry.value),
		}
		batches.Entries = append(batches.Entries, s)
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

func (b *BucketStore) Series(queryCtx logsgoql.QueryContext, plan *logsgoql.Plan) ([]logsgoql.Series, error) {
	iterStart, iterEnd := seriesIterWindow(queryCtx)

	seriesByKey := make(map[LogKey][]logsgoql.Sample)

	blocks, err := b.selectBlocksForWindow(plan, iterStart, iterEnd)
	if err != nil {
		return nil, err
	}
	for _, blk := range blocks {
		if err := b.appendSeriesFromBlock(plan, iterStart, iterEnd, blk.key, seriesByKey); err != nil {
			return nil, err
		}
	}

	results := make([]logsgoql.Series, 0, len(seriesByKey))
	for k, pts := range seriesByKey {
		sort.Slice(pts, func(i, j int) bool { return pts[i].Timestamp < pts[j].Timestamp })
		pts = compactSamplesByTimestamp(pts)

		results = append(results, logsgoql.Series{
			Service: k.Service,
			Level:   k.Level,
			Message: k.Message,
			Points:  pts,
		})
	}

	return results, nil
}

func (b *BucketStore) appendSeriesFromBlock(plan *logsgoql.Plan, iterStart, iterEnd int64, blockKey string, seriesByKey map[LogKey][]logsgoql.Sample) error {
	entries, err := b.loadSeriesFromBlock(blockKey)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if e == nil || e.Entry == nil {
			continue
		}
		ts := e.Entry.Timestamp
		if ts < iterStart || ts > iterEnd {
			continue
		}

		service := e.Entry.Service
		level := e.Entry.Level
		message := e.Entry.Message

		matched, err := plan.Match(logsgoql.EntryLabels{
			Service: service,
			Level:   level,
			Message: message,
		})
		if err != nil {
			return err
		}
		if !matched {
			continue
		}

		logKey := LogKey{Service: service, Level: level, Message: message}
		seriesByKey[logKey] = append(seriesByKey[logKey], logsgoql.Sample{
			Timestamp: ts,
			Count:     e.Count,
		})
	}
	return nil
}

// requiredServiceValues returns a list of service values when the plan implies the query
// must match one of those services.
//
// This is used as a safe pushdown optimization for bucket queries. Returns
// (nil,false) when the query is not safely service-constrained.
func requiredServiceValues(n logsgoql.Node) ([]string, bool) {
	set, ok := requiredServiceSet(n)
	if !ok {
		return nil, false
	}
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	return out, true
}

func requiredServiceSet(n logsgoql.Node) (map[string]struct{}, bool) {
	switch x := n.(type) {
	case *logsgoql.MatchNode:
		if x.Field == logsgoql.FieldService && x.Op == logsgoql.MatchEq {
			return map[string]struct{}{x.Value: {}}, true
		}
		return nil, false
	case *logsgoql.BinaryNode:
		left, leftOK := requiredServiceSet(x.Left)
		right, rightOK := requiredServiceSet(x.Right)

		switch x.Op {
		case logsgoql.OpAnd:
			switch {
			case leftOK && rightOK:
				// Intersection.
				if len(left) == 0 || len(right) == 0 {
					return map[string]struct{}{}, true
				}
				out := make(map[string]struct{})
				for s := range left {
					if _, ok := right[s]; ok {
						out[s] = struct{}{}
					}
				}
				return out, true
			case leftOK:
				return left, true
			case rightOK:
				return right, true
			default:
				return nil, false
			}
		case logsgoql.OpOr:
			if !leftOK || !rightOK {
				return nil, false
			}
			// Union.
			out := make(map[string]struct{}, len(left)+len(right))
			for s := range left {
				out[s] = struct{}{}
			}
			for s := range right {
				out[s] = struct{}{}
			}
			return out, true
		default:
			return nil, false
		}
	default:
		return nil, false
	}
}

func parseBlockKey(key string) (minT, maxT int64, service string, ok bool) {
	parts := strings.SplitN(key, "/", 2)
	if len(parts) != 2 {
		return 0, 0, "", false
	}
	rng := strings.SplitN(parts[0], "-", 2)
	if len(rng) != 2 {
		return 0, 0, "", false
	}
	minV, err := strconv.ParseInt(rng[0], 10, 64)
	if err != nil {
		return 0, 0, "", false
	}
	maxV, err := strconv.ParseInt(rng[1], 10, 64)
	if err != nil {
		return 0, 0, "", false
	}
	name := parts[1]
	u := strings.IndexByte(name, '_')
	if u <= 0 {
		return 0, 0, "", false
	}
	svc := name[:u]
	return minV, maxV, svc, true
}

type bucketBlockInfo struct {
	key  string
	minT int64
	maxT int64
}

func (b *BucketStore) selectBlocksForWindow(plan *logsgoql.Plan, iterStart, iterEnd int64) ([]bucketBlockInfo, error) {
	// Decide which services to consider without listing all objects if possible.
	services, servicesConstrained := requiredServiceValues(plan.Root)
	if servicesConstrained && len(services) == 0 {
		// Query implies an impossible service constraint (e.g. service="a" AND service="b").
		return nil, nil
	}

	// If we aren't service constrained, try to enumerate services from meta.json first.
	if !servicesConstrained {
		meta, err := b.GetMetaData()
		if err == nil && meta != nil && len(meta.Services) > 0 {
			for svc := range meta.Services {
				services = append(services, svc)
			}
			sort.Strings(services)
			servicesConstrained = true
		}
	}

	var blocks []bucketBlockInfo
	var fallbackServices []string

	if servicesConstrained && len(services) > 0 {
		seen := make(map[string]struct{})
		for _, svc := range services {
			entries, err := b.findBestCompactedBlocks(svc, iterStart, iterEnd)
			if err != nil || entries == nil {
				fallbackServices = append(fallbackServices, svc)
				continue
			}
			for _, e := range entries {
				if _, ok := seen[e.Key]; ok {
					continue
				}
				seen[e.Key] = struct{}{}
				blocks = append(blocks, bucketBlockInfo{key: e.Key, minT: e.MinTS, maxT: e.MaxTS})
			}
		}
	}

	// Fallback: list objects when we couldn't get full coverage via compacted blocks.
	if !servicesConstrained || len(fallbackServices) > 0 {
		fallbackSet := make(map[string]struct{})
		if servicesConstrained {
			for _, svc := range fallbackServices {
				fallbackSet[svc] = struct{}{}
			}
		}

		seen := make(map[string]struct{}, len(blocks))
		for _, bi := range blocks {
			seen[bi.key] = struct{}{}
		}

		objectCh := b.client.ListObjects(b.ctx, b.config.Bucket, minio.ListObjectsOptions{
			Recursive: true,
		})

		for object := range objectCh {
			if object.Err != nil {
				return nil, object.Err
			}
			if !strings.HasSuffix(object.Key, ".pb") {
				continue
			}
			minT, maxT, svc, ok := parseBlockKey(object.Key)
			if ok {
				if maxT < iterStart || minT > iterEnd {
					continue
				}
				if servicesConstrained {
					if _, keep := fallbackSet[svc]; !keep {
						continue
					}
				}
			} else if servicesConstrained {
				continue
			}
			if _, ok := seen[object.Key]; ok {
				continue
			}
			seen[object.Key] = struct{}{}
			blocks = append(blocks, bucketBlockInfo{key: object.Key, minT: minT, maxT: maxT})
		}
	}

	sort.Slice(blocks, func(i, j int) bool {
		// When minT isn't known (0), fall back to key ordering.
		if blocks[i].minT == blocks[j].minT {
			return blocks[i].key < blocks[j].key
		}
		return blocks[i].minT < blocks[j].minT
	})

	return blocks, nil
}

func (b *BucketStore) SeriesRange(queryCtx logsgoql.QueryContext, plan *logsgoql.Plan, resolution int64) ([]logsgoql.Series, error) {
	if resolution <= 0 || queryCtx.EndTs == queryCtx.StartTs {
		return b.Series(queryCtx, plan)
	}

	iterStart, iterEnd := seriesIterWindow(queryCtx)

	steps := make([]int64, 0, 1+((queryCtx.EndTs-queryCtx.StartTs)/resolution))
	for t := queryCtx.StartTs; t <= queryCtx.EndTs; t += resolution {
		steps = append(steps, t)
	}
	if len(steps) == 0 {
		return nil, nil
	}

	blocks, err := b.selectBlocksForWindow(plan, iterStart, iterEnd)
	if err != nil {
		return nil, err
	}

	type rangeState struct {
		nextStep int
		hasLast  bool
		lastTs   int64
		lastVal  uint64
	}

	stateByKey := make(map[LogKey]*rangeState)
	outByKey := make(map[LogKey][]logsgoql.Sample)

	for _, blk := range blocks {
		entries, err := b.loadSeriesFromBlock(blk.key)
		if err != nil {
			return nil, err
		}
		sort.Slice(entries, func(i, j int) bool {
			if entries[i] == nil || entries[i].Entry == nil {
				return false
			}
			if entries[j] == nil || entries[j].Entry == nil {
				return true
			}
			return entries[i].Entry.Timestamp < entries[j].Entry.Timestamp
		})

		for _, e := range entries {
			if e == nil || e.Entry == nil {
				continue
			}
			ts := e.Entry.Timestamp
			if ts < iterStart || ts > iterEnd {
				continue
			}

			service := e.Entry.Service
			level := e.Entry.Level
			message := e.Entry.Message

			matched, err := plan.Match(logsgoql.EntryLabels{
				Service: service,
				Level:   level,
				Message: message,
			})
			if err != nil {
				return nil, err
			}
			if !matched {
				continue
			}

			key := LogKey{Service: service, Level: level, Message: message}
			st := stateByKey[key]
			if st == nil {
				st = &rangeState{}
				stateByKey[key] = st
			}

			// Finalize steps strictly before this sample using the previous last value.
			for st.nextStep < len(steps) && steps[st.nextStep] < ts {
				stepT := steps[st.nextStep]
				minT := stepT - queryCtx.Lookback
				if minT < 0 {
					minT = 0
				}
				if st.hasLast && st.lastTs >= minT {
					outByKey[key] = append(outByKey[key], logsgoql.Sample{
						Timestamp: stepT,
						Count:     st.lastVal,
					})
				}
				st.nextStep++
			}

			st.hasLast = true
			st.lastTs = ts
			st.lastVal = e.Count
		}
	}

	// Finalize remaining steps after processing all samples.
	for key, st := range stateByKey {
		for st.nextStep < len(steps) {
			stepT := steps[st.nextStep]
			minT := stepT - queryCtx.Lookback
			if minT < 0 {
				minT = 0
			}
			if st.hasLast && st.lastTs <= stepT && st.lastTs >= minT {
				outByKey[key] = append(outByKey[key], logsgoql.Sample{
					Timestamp: stepT,
					Count:     st.lastVal,
				})
			}
			st.nextStep++
		}
	}

	results := make([]logsgoql.Series, 0, len(outByKey))
	for k, pts := range outByKey {
		pts = compactSamplesByTimestamp(pts)
		if len(pts) == 0 {
			continue
		}
		results = append(results, logsgoql.Series{
			Service: k.Service,
			Level:   k.Level,
			Message: k.Message,
			Points:  pts,
		})
	}

	return results, nil
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
	b.mu.Lock()
	defer b.mu.Unlock()
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
