package logclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"github.com/Saumya40-codes/LogsGO/api/rest"
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
	factory := pkg.IngestionFactory{
		DataDir:       t.TempDir(),
		MaxTimeInMem:  "10s", // no need to keep it realistic, but a sensible value should be enough, eh !?
		UnLockDataDir: true,
	}
	ctx := context.Background()
	serv := ingestion.NewLogIngestorServer(&factory)
	go ingestion.StartServer(ctx, serv)
	go rest.StartServer(serv)

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

	res, err := http.Get("http://localhost:8080/query?service=ap-south1")
	testutil.Ok(t, err)

	fmt.Println(res)

	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Fatalf("Read error: %v", err)
	}

	var logs []*logapi.LogEntry
	if err := json.Unmarshal(body, &logs); err != nil {
		log.Fatalf("JSON unmarshal error: %v", err)
		return
	}

	for _, logEntry := range logs {
		fmt.Printf("[%s] %s: %s\n", logEntry.Level, logEntry.Service, logEntry.Message)
	}

	testutil.Assert(t, len(logs) == noOfSamples, "Expected %d log but got %d", noOfSamples, len(logs))
}
