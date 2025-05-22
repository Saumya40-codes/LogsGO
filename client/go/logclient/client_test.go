package logclient

import (
	"context"
	"testing"
	"time"

	"github.com/Saumya40-codes/LogsGO/internal/ingestion"
	"github.com/Saumya40-codes/LogsGO/pkg"
	"github.com/efficientgo/core/testutil"
)

func TestGRPCConn(t *testing.T) {
	factory := pkg.IngestionFactory{
		DataDir:      "data",
		MaxTimeInMem: "1h",
	}
	go ingestion.StartServer(&factory)

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
