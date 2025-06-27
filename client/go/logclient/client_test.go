package logclient

import (
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Saumya40-codes/LogsGO/api/rest"
	"github.com/Saumya40-codes/LogsGO/internal/ingestion"
	"github.com/Saumya40-codes/LogsGO/pkg"
	"github.com/Saumya40-codes/LogsGO/pkg/store"
	"github.com/efficientgo/core/testutil"
	"github.com/efficientgo/e2e"
	e2edb "github.com/efficientgo/e2e/db"
	"gopkg.in/yaml.v3"
)

var factory = pkg.IngestionFactory{ // we wait 2 seconds before starting flush monitor i.e. ideally maxtimeinmem should be more than 2 seconds then its set
	MaxTimeInMem:     "8s", // no need to keep it realistic, but a sensible value should be enough, eh !?
	UnLockDataDir:    true,
	HttpListenAddr:   ":8080",
	MaxRetentionTime: "10d", // this is the time after which logs will be deleted from disk
	WebListenAddr:    "*",
	LookbackPeriod:   "15m",
	GrpcListenAddr:   ":50051",
}

// removes s3 related stuff
func cleanupFactory() {
	factory.StoreConfig = ""
	factory.StoreConfigPath = ""
	factory.MaxRetentionTime = "10d"
}

type expectedLog struct {
	Level   string
	Service string
	Message string
	Count   int
}

func verifyLogs(t *testing.T, url string, expected []expectedLog) {
	resp, err := http.Get(url)
	testutil.Ok(t, err, "Failed to query logs from REST API")

	defer resp.Body.Close()
	testutil.Assert(t, resp.StatusCode == http.StatusOK, "Expected status 200 OK, got %d", resp.StatusCode)

	var actual []store.QueryResponse
	err = json.NewDecoder(resp.Body).Decode(&actual)
	testutil.Ok(t, err, "Failed to decode query response")

	testutil.Assert(t, len(actual) >= len(expected), "Expected at least %d logs, got %d", len(expected), len(actual))

	for _, exp := range expected {
		found := false
		for _, act := range actual {
			if act.Level == exp.Level && act.Service == exp.Service && act.Message == exp.Message {
				testutil.Assert(t, act.Series[0].Count == uint64(exp.Count), "Expected count %d, got %d", exp.Count, act.Series[0].Count)
				found = true
				break
			}
		}
		testutil.Assert(t, found, "Expected log %+v not found in response", exp)
	}
}

func TestGRPCConn(t *testing.T) {
	factory.DataDir = t.TempDir()
	ctx := t.Context()
	serv := ingestion.NewLogIngestorServer(ctx, &factory)
	go ingestion.StartServer(ctx, serv, factory.GrpcListenAddr, false, "")

	// waiting for server to start
	time.Sleep(2 * time.Second)

	opts := &Opts{
		Message: "Time duration execeeded",
		Level:   "warn",
		Service: "ap-south1",
	}

	lc, err := NewLogClient(ctx, factory.GrpcListenAddr)
	testutil.Ok(t, err)

	ok := lc.UploadLog(opts)

	testutil.Assert(t, ok, "logs can't be uploaded")
}

func TestDirCreated(t *testing.T) {
	factory.DataDir = t.TempDir()
	ctx := t.Context()
	serv := ingestion.NewLogIngestorServer(ctx, &factory)
	go ingestion.StartServer(ctx, serv, factory.GrpcListenAddr, false, "")
	go rest.StartServer(ctx, serv, &factory)

	// waiting for server to start
	time.Sleep(2 * time.Second)

	opts := &Opts{
		Message: "Time duration execeeded",
		Level:   "warn",
		Service: "ap-south1",
	}

	lc, err := NewLogClient(ctx, factory.GrpcListenAddr)
	testutil.Ok(t, err)

	ok := lc.UploadLog(opts)

	testutil.Assert(t, ok, "logs can't be uploaded")

	// we should have 'atleast' something in data/
	checkDirExists(t, factory.DataDir)
}

func checkDirExists(t *testing.T, path string) {
	info, err := os.Stat(path)

	testutil.Ok(t, err, "Path including /data/* should have existed")
	testutil.Assert(t, info.IsDir(), "/data/* dir doesn't exists")

	// do same for data/index now
	indexPath := path + "/index"

	info, err = os.Stat(indexPath)

	testutil.Ok(t, err, "Path including /data/* should have existed")
	testutil.Assert(t, info.IsDir(), "/data/* dir doesn't exists")
}

func TestLabelValues(t *testing.T) {
	factory.DataDir = t.TempDir()
	ctx := t.Context()
	serv := ingestion.NewLogIngestorServer(ctx, &factory)
	go ingestion.StartServer(ctx, serv, factory.GrpcListenAddr, false, "")
	go rest.StartServer(ctx, serv, &factory)

	// waiting for server to start
	time.Sleep(2 * time.Second)

	opts := &Opts{
		Message: "Time duration execeeded",
		Level:   "warn",
		Service: "ap-south1",
	}

	lc, err := NewLogClient(ctx, factory.GrpcListenAddr)
	testutil.Ok(t, err)

	ok := lc.UploadLog(opts)

	testutil.Assert(t, ok, "logs can't be uploaded")
	// there should be persistance in memory store

	resp, err := http.Get("http://localhost:8080/api/v1/labels")
	testutil.Ok(t, err, "Failed to get label values from REST API")
	defer resp.Body.Close()

	testutil.Assert(t, resp.StatusCode == http.StatusOK, "Expected status code 200 OK, got %d", resp.StatusCode)
	var labels rest.LabelValuesResponse

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&labels)

	testutil.Ok(t, err, "Failed to decode label values from response")
	expectedServices := []string{"ap-south1"}
	expectedLevels := []string{"warn"}
	AssertLabels(t, labels, expectedServices, expectedLevels)

	time.Sleep(9 * time.Second)
	// now we should have flushed logs to disk, so we should have same labels
	resp, err = http.Get("http://localhost:8080/api/v1/labels")
	testutil.Ok(t, err, "Failed to get label values from REST API after flushing")
	defer resp.Body.Close()

	testutil.Assert(t, resp.StatusCode == http.StatusOK, "Expected status code 200 OK, got %d", resp.StatusCode)
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&labels)
	testutil.Ok(t, err, "Failed to decode label values from response after flushing")

	AssertLabels(t, labels, expectedServices, expectedLevels)
}

func AssertLabels(t *testing.T, labels rest.LabelValuesResponse, expectedServices []string, expectedLevels []string) {
	testutil.Assert(t, len(labels.Services) == len(expectedServices), "Expected %d service labels, got %d", len(expectedServices), len(labels.Services))
	testutil.Assert(t, len(labels.Levels) == len(expectedLevels), "Expected %d level labels, got %d", len(expectedLevels), len(labels.Levels))

	for _, service := range expectedServices {
		testutil.Assert(t, store.Contains(labels.Services, service), "Expected service label %s not found", service)
	}

	for _, level := range expectedLevels {
		testutil.Assert(t, store.Contains(labels.Levels, level), "Expected level label %s not found", level)
	}
}

func TestQueryOutput(t *testing.T) {
	factory.DataDir = t.TempDir()
	ctx := t.Context()
	serv := ingestion.NewLogIngestorServer(ctx, &factory)
	go ingestion.StartServer(ctx, serv, factory.GrpcListenAddr, false, "")
	go rest.StartServer(ctx, serv, &factory)

	time.Sleep(2 * time.Second) // wait for servers

	opts := &Opts{
		Message: "Time duration execeeded",
		Level:   "warn",
		Service: "ap-south1",
	}

	lc, err := NewLogClient(ctx, factory.GrpcListenAddr)
	testutil.Ok(t, err)

	testutil.Assert(t, lc.UploadLog(opts), "logs can't be uploaded")
	time.Sleep(2 * time.Second)

	// Check log in memory
	verifyLogs(t, `http://localhost:8080/api/v1/query?expression=level="warn"&start=0&end=0&resolution=0s`, []expectedLog{
		{Level: "warn", Service: "ap-south1", Message: "Time duration execeeded", Count: 1},
	})

	time.Sleep(9 * time.Second) // logs flushed to disk

	// Check log after flush
	verifyLogs(t, `http://localhost:8080/api/v1/query?expression=level="warn"&start=0&end=0&resolution=0s`, []expectedLog{
		{Level: "warn", Service: "ap-south1", Message: "Time duration execeeded", Count: 1},
	})

	// Upload log again (should increment count)
	testutil.Assert(t, lc.UploadLog(opts), "logs can't be uploaded again")

	// Check log count = 2
	verifyLogs(t, `http://localhost:8080/api/v1/query?expression=level="warn"&start=0&end=0&resolution=0s`, []expectedLog{
		{Level: "warn", Service: "ap-south1", Message: "Time duration execeeded", Count: 2},
	})
}

func TestLogDataUploadToS3(t *testing.T) {
	// start minio server and also docker env
	e, err := e2e.NewDockerEnvironment("uploadS3test")
	testutil.Ok(t, err)
	t.Cleanup(func() {
		cleanupFactory()
		e.Close()
	})

	m1 := e2edb.NewMinio(e, "minio-1", "default")
	testutil.Ok(t, e2e.StartAndWaitReady(m1))

	bktConfig, err := yaml.Marshal(store.Config{
		RemoteStore: store.BucketStoreConfig{
			Provider:            "minio",
			Bucket:              "bkt1",
			CreateBucketOnEmpty: true,
			Endpoint:            m1.Endpoint("http"),
			SecretKey:           e2edb.MinioSecretKey,
			AccessKey:           e2edb.MinioAccessKey,
		},
	})
	testutil.Ok(t, err)

	factory.DataDir = t.TempDir()
	factory.MaxRetentionTime = "15s"
	factory.StoreConfig = string(bktConfig)

	ctx := t.Context()
	serv := ingestion.NewLogIngestorServer(ctx, &factory)
	go ingestion.StartServer(ctx, serv, factory.GrpcListenAddr, false, "")
	go rest.StartServer(ctx, serv, &factory)

	// waiting for server to start
	time.Sleep(2 * time.Second)

	opts := &Opts{
		Message: "Time duration execeeded",
		Level:   "warn",
		Service: "ap-south1",
	}

	newOpt := &Opts{
		Message: "Notification has been sent",
		Level:   "info",
		Service: "myService",
	}

	lc, err := NewLogClient(ctx, factory.GrpcListenAddr)
	testutil.Ok(t, err)

	ok := lc.UploadLog(opts)
	ok1 := lc.UploadLog(newOpt)
	testutil.Assert(t, ok && ok1, "logs can't be uploaded")

	time.Sleep(20 * time.Second) // TODO: this is time consuming but can't figure out better way, so adding t.Parallel's would do the job

	// query logs now (we do this instead of labelvalues as underlying implementation to fetch is same for now)
	resp, err := http.Get(`http://localhost:8080/api/v1/query?expression=level="warn"&start=0&end=0&resolution=0s`)
	testutil.Ok(t, err, "Failed to get query output from REST API after flushing")
	defer resp.Body.Close()
	testutil.Assert(t, resp.StatusCode == http.StatusOK, "Expected status code 200 OK, got %d", resp.StatusCode)
	var queryOutputAfterFlush []store.QueryResponse
	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&queryOutputAfterFlush)
	testutil.Ok(t, err, "Failed to decode query output from response after flushing")
	testutil.Assert(t, len(queryOutputAfterFlush) > 0, "Expected at least one log entry in query output after flushing, got %d", len(queryOutputAfterFlush))
	for _, log := range queryOutputAfterFlush {
		testutil.Assert(t, log.Level == "warn", "Expected log level 'warn', got '%s'", log.Level)
		testutil.Assert(t, log.Service == "ap-south1", "Expected log service 'ap-south1', got '%s'", log.Service)
		testutil.Assert(t, log.Message == "Time duration execeeded", "Expected log message 'Time duration execeeded', got '%s'", log.Message)
		testutil.Assert(t, log.Series[0].Count == 1, "Expected log counter to have value 1 got '%d'", log.Series[0].Count)
	}

	base := "http://localhost:8080/api/v1/query"
	params := url.Values{}
	params.Set("expression", "level=warn&level=info")
	params.Set("start", "0")
	params.Set("end", "0")
	params.Set("resolution", "0s")
	fullURL := base + "?" + params.Encode()

	resp, err = http.Get(fullURL)
	testutil.Ok(t, err, "Failed to get query output from REST API after flushing")
	defer resp.Body.Close()
	testutil.Assert(t, resp.StatusCode == http.StatusOK, "Expected status code 200 OK, got %d", resp.StatusCode)
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&queryOutputAfterFlush)
	testutil.Ok(t, err, "Failed to decode query output from response after flushing")
	testutil.Assert(t, len(queryOutputAfterFlush) == 0, "Expected no log entry in query output after flushing, got %d", len(queryOutputAfterFlush))

	// check for label values now
	resp, err = http.Get("http://localhost:8080/api/v1/labels")
	testutil.Ok(t, err, "Failed to get label values from REST API after flushing")
	defer resp.Body.Close()
	testutil.Assert(t, resp.StatusCode == http.StatusOK, "Expected status code 200 OK, got %d", resp.StatusCode)
	var labels rest.LabelValuesResponse
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&labels)
	testutil.Ok(t, err, "Failed to decode label values from response after flushing")
	testutil.Assert(t, len(labels.Services) == 2, "Expected 2 service labels, got %d", len(labels.Services))
	testutil.Assert(t, len(labels.Levels) == 2, "Expected 2 level labels, got %d", len(labels.Levels))
}

func TestRangeQueries(t *testing.T) {
	factory.DataDir = t.TempDir()
	ctx := t.Context()
	serv := ingestion.NewLogIngestorServer(ctx, &factory)
	go ingestion.StartServer(ctx, serv, factory.GrpcListenAddr, false, "")
	go rest.StartServer(ctx, serv, &factory)

	time.Sleep(2 * time.Second) // wait for servers
	startTs := time.Now()

	opts := &Opts{
		Message:   "Time duration execeeded",
		Level:     "warn",
		Service:   "ap-south1",
		TimeStamp: startTs.Unix(),
	}

	lc, err := NewLogClient(ctx, factory.GrpcListenAddr)
	testutil.Ok(t, err)

	ok := lc.UploadLog(opts)
	testutil.Assert(t, ok, "logs can't be uploaded")

	// add logs +15min after startTs
	newOpts := opts
	newOpts.TimeStamp = startTs.Add(15 * time.Minute).Unix()
	ok = lc.UploadLog(newOpts)
	testutil.Assert(t, ok, "logs can't be uploaded")

	// add logs +30min after startTs
	newOpts.TimeStamp = startTs.Add(30 * time.Minute).Unix()
	ok = lc.UploadLog(newOpts)
	testutil.Assert(t, ok, "logs can't be uploaded")

	// perform range query for 15min interval
	queryStart := startTs.Unix()
	queryEnd := startTs.Add(30 * time.Minute).Unix()
	resolution := "15m"

	base := "http://localhost:8080/api/v1/query"
	params := url.Values{}
	params.Set("expression", "level=warn")
	params.Set("start", strconv.FormatInt(queryStart, 10))
	params.Set("end", strconv.FormatInt(queryEnd, 10))
	params.Set("resolution", resolution)
	fullURL := base + "?" + params.Encode()
	resp, err := http.Get(fullURL)
	testutil.Ok(t, err, "Failed to get query output from REST API after flushing")
	defer resp.Body.Close()
	testutil.Assert(t, resp.StatusCode == http.StatusOK, "Expected status code 200 OK, got %d", resp.StatusCode)

	var queryOutput []store.QueryResponse
	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&queryOutput)

	testutil.Ok(t, err, "Failed to decode query output from response after flushing")
	testutil.Assert(t, len(queryOutput) == 1, "Expected 1 log entry in query output after flushing, got %d", len(queryOutput))
	testutil.Assert(t, queryOutput[0].Level == "warn", "Expected log level 'warn', got '%s'", queryOutput[0].Level)
	testutil.Assert(t, queryOutput[0].Service == "ap-south1", "Expected log service 'ap-south1', got '%s'", queryOutput[0].Service)
	testutil.Assert(t, queryOutput[0].Message == "Time duration execeeded", "Expected log message 'Time duration execeeded', got '%s'", queryOutput[0].Message)

	testutil.Assert(t, queryOutput[0].Series[0].Timestamp == queryStart, "Expected log timestamp to be %d, got %d", queryStart, queryOutput[0].Series[0].Timestamp)
	testutil.Assert(t, queryOutput[0].Series[0].Count == 1, "Expected log counter to have value 1, got %d", queryOutput[0].Series[0].Count)

	testutil.Assert(t, queryOutput[0].Series[1].Timestamp == queryStart+15*60, "Expected log timestamp to be %d, got %d", queryStart+15*60, queryOutput[0].Series[1].Timestamp)
	testutil.Assert(t, queryOutput[0].Series[1].Count == 2, "Expected log counter to have value 2, got %d", queryOutput[0].Series[1].Count)

	testutil.Assert(t, queryOutput[0].Series[2].Timestamp == queryStart+30*60, "Expected log timestamp to be %d, got %d", queryStart+30*60, queryOutput[0].Series[2].Timestamp)
	testutil.Assert(t, queryOutput[0].Series[2].Count == 3, "Expected log counter to have value 3, got %d", queryOutput[0].Series[2].Count)
}
