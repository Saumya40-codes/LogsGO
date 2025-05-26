package logclient

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Saumya40-codes/LogsGO/internal/ingestion"
	"github.com/Saumya40-codes/LogsGO/pkg"
	"github.com/efficientgo/core/testutil"
)

func TestGRPCConn(t *testing.T) {
	factory := pkg.IngestionFactory{
		DataDir:      t.TempDir(),
		MaxTimeInMem: "1h",
	}
	ctx := t.Context()
	go ingestion.StartServer(ctx, &factory)

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
	factory := pkg.IngestionFactory{
		DataDir:      t.TempDir(),
		MaxTimeInMem: "30s", // no need to keep it realistic, but a sensible value should be enough, eh !?
		LockDataDir:  true,
	}
	ctx := context.Background()
	go ingestion.StartServer(ctx, &factory)

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
	checkDirExists(t, factory.DataDir)
	time.Sleep(35 * time.Second)
	// there should be persistance
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
