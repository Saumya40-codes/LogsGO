// Chaos load generator for LogsGO — scenarios exercise ingestion, queries, and flush paths.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Saumya40-codes/LogsGO/client/go/logclient"
)

var (
	grpcAddr  = flag.String("grpc", "127.0.0.1:50051", "gRPC address")
	httpAddr  = flag.String("http", "http://127.0.0.1:8080", "HTTP base URL")
	scenario  = flag.String("scenario", "all", "scenario: ingest|batch|query|mixed|cardinality|burst|all")
	duration  = flag.Duration("duration", 45*time.Second, "run duration")
	workers   = flag.Int("workers", 32, "concurrent workers")
	batchSize = flag.Int("batch", 50, "batch size for batch scenario")
	services  = flag.Int("services", 20, "distinct services")
	levels    = []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}
	messages  = []string{
		"request completed", "db timeout", "cache miss", "auth failed",
		"queue lag high", "retrying upstream", "payload too large", "slow query",
	}
)

func main() {
	flag.Parse()
	ctx := context.Background()
	client, err := logclient.NewLogClient(ctx, *grpcAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "client: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	scenarios := []string{*scenario}
	if *scenario == "all" {
		scenarios = []string{"ingest", "batch", "cardinality", "burst", "query", "mixed"}
	}

	for _, sc := range scenarios {
		fmt.Printf("\n=== scenario=%s workers=%d duration=%s ===\n", sc, *workers, *duration)
		runScenario(ctx, client, sc)
		time.Sleep(2 * time.Second)
	}
}

func runScenario(ctx context.Context, client *logclient.Client, sc string) {
	deadline := time.Now().Add(*duration)
	var ok, fail, queries atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*97))
			for time.Now().Before(deadline) {
				switch sc {
				case "ingest":
					if err := client.UploadLog(ctx, randomLog(r, false)); err != nil {
						fail.Add(1)
					} else {
						ok.Add(1)
					}
				case "batch":
					entries := make([]*logclient.LogOpts, *batchSize)
					for j := range entries {
						entries[j] = randomLog(r, false)
					}
					if err := client.UploadLogs(ctx, &logclient.LogBatch{Entries: entries}); err != nil {
						fail.Add(1)
					} else {
						ok.Add(int64(*batchSize))
					}
				case "cardinality":
					// high label cardinality / unique messages
					if err := client.UploadLog(ctx, randomLog(r, true)); err != nil {
						fail.Add(1)
					} else {
						ok.Add(1)
					}
				case "burst":
					// short bursts of batches
					for b := 0; b < 5; b++ {
						entries := make([]*logclient.LogOpts, *batchSize)
						for j := range entries {
							entries[j] = randomLog(r, false)
						}
						if err := client.UploadLogs(ctx, &logclient.LogBatch{Entries: entries}); err != nil {
							fail.Add(1)
						} else {
							ok.Add(int64(*batchSize))
						}
					}
					time.Sleep(50 * time.Millisecond)
				case "query":
					// seed a little then query
					_ = client.UploadLog(ctx, randomLog(r, false))
					ok.Add(1)
					if err := doQuery(r); err != nil {
						fail.Add(1)
					} else {
						queries.Add(1)
					}
				case "mixed":
					switch r.Intn(4) {
					case 0:
						if err := client.UploadLog(ctx, randomLog(r, false)); err != nil {
							fail.Add(1)
						} else {
							ok.Add(1)
						}
					case 1, 2:
						entries := make([]*logclient.LogOpts, *batchSize/2+1)
						for j := range entries {
							entries[j] = randomLog(r, r.Intn(10) == 0)
						}
						if err := client.UploadLogs(ctx, &logclient.LogBatch{Entries: entries}); err != nil {
							fail.Add(1)
						} else {
							ok.Add(int64(len(entries)))
						}
					default:
						if err := doQuery(r); err != nil {
							fail.Add(1)
						} else {
							queries.Add(1)
						}
					}
				}
			}
		}(i)
	}
	wg.Wait()
	fmt.Printf("ok_logs=%d fail_ops=%d queries_ok=%d\n", ok.Load(), fail.Load(), queries.Load())
}

func randomLog(r *rand.Rand, highCard bool) *logclient.LogOpts {
	svc := fmt.Sprintf("svc-%d", r.Intn(*services))
	msg := messages[r.Intn(len(messages))]
	labels := map[string]string{
		"env":    []string{"prod", "staging", "dev"}[r.Intn(3)],
		"region": []string{"us-east", "eu-west", "ap-south"}[r.Intn(3)],
	}
	if highCard {
		msg = fmt.Sprintf("%s id=%d nonce=%d", msg, r.Int63(), r.Int63())
		labels["trace_id"] = fmt.Sprintf("%016x", r.Uint64())
		labels["user_id"] = strconv.Itoa(r.Intn(100000))
	}
	return &logclient.LogOpts{
		Service:   svc,
		Level:     levels[r.Intn(len(levels))],
		Message:   msg,
		Timestamp: time.Now().Unix(),
		Labels:    labels,
	}
}

func doQuery(r *rand.Rand) error {
	svc := fmt.Sprintf("svc-%d", r.Intn(*services))
	level := levels[r.Intn(len(levels))]
	expr := url.QueryEscape(fmt.Sprintf(`service="%s" AND level="%s"`, svc, level))
	end := time.Now().Unix()
	start := end - 300
	// range query
	u := fmt.Sprintf("%s/api/v1/query?expression=%s&start=%d&end=%d&resolution=30s", *httpAddr, expr, start, end)
	resp, err := http.Get(u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("query status %d", resp.StatusCode)
	}
	// also labels endpoint
	resp2, err := http.Get(*httpAddr + "/api/v1/labels")
	if err != nil {
		return err
	}
	defer resp2.Body.Close()
	_, _ = io.Copy(io.Discard, resp2.Body)
	return nil
}
