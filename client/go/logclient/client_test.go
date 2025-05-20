package logclient

import (
	"testing"
	"time"

	"github.com/Saumya40-codes/LogsGO/internal/ingestion"
)

func TestGRPCConn(t *testing.T) {
	go ingestion.StartServer()

	// wait for server to start
	time.Sleep(2 * time.Second)

	if ok := UploadLogs(); !ok {
		t.Fatal("upload failed")
	}
}
