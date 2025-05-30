package logclient

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/Saumya40-codes/LogsGO/api/rest"
	"github.com/Saumya40-codes/LogsGO/internal/ingestion"
	"github.com/Saumya40-codes/LogsGO/pkg"
	"github.com/efficientgo/core/testutil"
)

var factory = pkg.IngestionFactory{
	MaxTimeInMem:     "10s", // no need to keep it realistic, but a sensible value should be enough, eh !?
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
	ctx := context.Background()
	serv := ingestion.NewLogIngestorServer(&factory)
	go ingestion.StartServer(ctx, serv)
	go rest.StartServer(serv, &factory)

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

	res, err := http.Get("http://localhost:8080/query?expression=ap-south1")
	testutil.Ok(t, err)
	testutil.Assert(t, res.StatusCode == http.StatusOK, "Expected status code 200, got %d", res.StatusCode)
	res.Body.Close()
}
