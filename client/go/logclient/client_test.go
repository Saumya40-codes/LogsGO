package logclient

import (
	"testing"
	"time"

	"github.com/Saumya40-codes/LogsGO/internal/ingestion"
	"github.com/Saumya40-codes/LogsGO/pkg"
)

func TestGRPCConn(t *testing.T) {
	factory := pkg.IngestionFactory{
		DataDir: "data",
	}
	go ingestion.StartServer(&factory)

	// wait for server to start
	time.Sleep(2 * time.Second)

	if ok := UploadLogs(); !ok {
		t.Fatal("upload failed")
	}
}
