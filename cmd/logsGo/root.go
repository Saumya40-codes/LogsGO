package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"unicode"

	"github.com/Saumya40-codes/LogsGO/api/rest"
	"github.com/Saumya40-codes/LogsGO/internal/ingestion"
	"github.com/Saumya40-codes/LogsGO/pkg"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "logsgo",
	Short: "Start the standalone LogsGo ingestion service",
	Run:   func(cmd *cobra.Command, args []string) {},
}

func main() {
	cfg := &pkg.IngestionFactory{}
	rootCmd.Flags().StringVar(&cfg.DataDir, "data-dir", "data", "Data directory path to store logs data. Default value is ./data")
	rootCmd.Flags().StringVar(&cfg.MaxRetentionTime, "max-retention-time", "10d", "Maximum time chunks blocks will remain in disk, default is 10d. \nSuffix the number with d->days m->months h->hours s->seconds")
	rootCmd.Flags().StringVar(&cfg.MaxTimeInMem, "max-time-in-mem", "1h", "Maximum time logs are in main memory, after which gets persisted to disk, default is 1h")
	rootCmd.Flags().BoolVar(&cfg.UnLockDataDir, "unlock-data-dir", false, "Use if you want to keep data directory unlocked for other process to perform operation on it. \n [WARNING] Recommended not to.")
	if !validateTimeDurations(cfg.MaxRetentionTime) || !validateTimeDurations(cfg.MaxTimeInMem) {
		log.Fatal("Invalid time duration set")
	}

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)

	wg := &sync.WaitGroup{}

	serv := ingestion.NewLogIngestorServer(cfg)

	wg.Add(1)
	go func() {
		defer wg.Done()
		ingestion.StartServer(ctx, serv)
	}()
	go rest.StartServer(serv)

	select {
	case <-ch:
		cancel()
	}
	wg.Wait()
}

func validateTimeDurations(dur string) bool {
	if len(dur) < 2 {
		return false
	}

	switch dur[len(dur)-1] {
	case 'd', 'h', 's', 'm':
	default:
		return false
	}

	if ch := rune(dur[len(dur)-2]); !unicode.IsDigit(ch) {
		return false
	}

	return true
}
