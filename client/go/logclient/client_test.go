package logclient

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/Saumya40-codes/LogsGO/api/rest"
	"github.com/Saumya40-codes/LogsGO/internal/ingestion"
	"github.com/Saumya40-codes/LogsGO/pkg"
	"github.com/Saumya40-codes/LogsGO/pkg/store"
	"github.com/efficientgo/core/testutil"
)

var factory = pkg.IngestionFactory{ // we wait 2 seconds before starting flush monitor i.e. ideally maxtimeinmem should be more than 2 seconds then its set
	MaxTimeInMem:     "8s", // no need to keep it realistic, but a sensible value should be enough, eh !?
	UnLockDataDir:    true,
	HttpListenAddr:   ":8080",
	MaxRetentionTime: "10d", // this is the time after which logs will be deleted from disk
}

func TestGRPCConn(t *testing.T) {
	factory.DataDir = t.TempDir()
	ctx := t.Context()
	serv := ingestion.NewLogIngestorServer(&factory)
	go ingestion.StartServer(ctx, serv)

	// waiting for server to start
	time.Sleep(2 * time.Second)

	opts := &Opts{
		Message: "Time duration execeeded",
		Level:   "warn",
		Service: "ap-south1",
	}

	lc, err := NewLogClient(context.Background())
	testutil.Ok(t, err)

	ok := lc.UploadLog(opts)

	testutil.Assert(t, ok, "logs can't be uploaded")
}

func TestLogsFlushedToDisk(t *testing.T) {
	factory.DataDir = t.TempDir()
	ctx := t.Context()
	serv := ingestion.NewLogIngestorServer(&factory)
	go ingestion.StartServer(ctx, serv)
	go rest.StartServer(ctx, serv, &factory)

	// waiting for server to start
	time.Sleep(2 * time.Second)

	opts := &Opts{
		Message: "Time duration execeeded",
		Level:   "warn",
		Service: "ap-south1",
	}

	lc, err := NewLogClient(context.Background())
	testutil.Ok(t, err)

	ok := lc.UploadLog(opts)

	testutil.Assert(t, ok, "logs can't be uploaded")

	// there shouldnt be persistance, we should have 'atleast' something in data/
	checkDirExists(t, factory.DataDir, 0)
	time.Sleep(9 * time.Second)
	// there should be persistance
	checkDirExists(t, factory.DataDir, 1)
}

func checkDirExists(t *testing.T, path string, noOfSamples int) {
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
	ctx := context.Background()
	serv := ingestion.NewLogIngestorServer(&factory)
	go ingestion.StartServer(ctx, serv)
	go rest.StartServer(ctx, serv, &factory)

	// waiting for server to start
	time.Sleep(2 * time.Second)

	opts := &Opts{
		Message: "Time duration execeeded",
		Level:   "warn",
		Service: "ap-south1",
	}

	lc, err := NewLogClient(context.Background())
	testutil.Ok(t, err)

	ok := lc.UploadLog(opts)

	testutil.Assert(t, ok, "logs can't be uploaded")
	time.Sleep(2 * time.Second)
	// there should be persistance in memory store

	resp, err := http.Get("http://localhost:8080/labels")
	testutil.Ok(t, err, "Failed to get label values from REST API")
	defer resp.Body.Close()

	testutil.Assert(t, resp.StatusCode == http.StatusOK, "Expected status code 200 OK, got %d", resp.StatusCode)
	var labels store.Labels

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&labels)

	testutil.Ok(t, err, "Failed to decode label values from response")
	expectedServices := []string{"ap-south1"}
	expectedLevels := []string{"warn"}
	AssertLabels(t, labels, expectedServices, expectedLevels)

	time.Sleep(9 * time.Second)
	// now we should have flushed logs to disk, so we should have same labels
	resp, err = http.Get("http://localhost:8080/labels")
	testutil.Ok(t, err, "Failed to get label values from REST API after flushing")
	defer resp.Body.Close()

	testutil.Assert(t, resp.StatusCode == http.StatusOK, "Expected status code 200 OK, got %d", resp.StatusCode)
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&labels)
	testutil.Ok(t, err, "Failed to decode label values from response after flushing")

	AssertLabels(t, labels, expectedServices, expectedLevels)
}

func AssertLabels(t *testing.T, labels store.Labels, expectedServices []string, expectedLevels []string) {
	testutil.Assert(t, len(labels.Services) == len(expectedServices), "Expected %d service labels, got %d", len(expectedServices), len(labels.Services))
	testutil.Assert(t, len(labels.Levels) == len(expectedLevels), "Expected %d level labels, got %d", len(expectedLevels), len(labels.Levels))

	for _, service := range expectedServices {
		testutil.Assert(t, store.Contains(labels.Services, service), "Expected service label %s not found", service)
	}

	for _, level := range expectedLevels {
		testutil.Assert(t, store.Contains(labels.Levels, level), "Expected level label %s not found", level)
	}
}
