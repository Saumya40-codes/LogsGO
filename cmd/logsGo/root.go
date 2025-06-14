package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"unicode"

	"github.com/Saumya40-codes/LogsGO/api/rest"
	"github.com/Saumya40-codes/LogsGO/internal/ingestion"
	"github.com/Saumya40-codes/LogsGO/pkg"
	"github.com/spf13/cobra"
)

var cfg = &pkg.IngestionFactory{}

var rootCmd = &cobra.Command{
	Use:   "logsgo",
	Short: "Start the standalone LogsGo ingestion service",
	Run: func(cmd *cobra.Command, args []string) {
		if !validateTimeDurations(cfg.MaxRetentionTime) || !validateTimeDurations(cfg.MaxTimeInMem) {
			log.Fatal("Invalid time duration set")
		}

		if cfg.StoreConfig != "" && cfg.StoreConfigPath != "" {
			log.Fatal("--store-config and --store-config-path flag can't be used together")
		}

		log.Println("Starting LogsGo ingestion service...")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)

		wg := &sync.WaitGroup{}
		serv := ingestion.NewLogIngestorServer(ctx, cfg)

		wg.Add(3)
		go func() {
			defer wg.Done()
			ingestion.StartServer(ctx, serv, cfg.GrpcListenAddr)
		}()

		go func() {
			defer wg.Done()
			rest.StartServer(ctx, serv, cfg)
		}()

		log.Println("Starting logsGo UI")
		addr := getAddr(cfg.WebListenAddr)
		go func() {
			defer wg.Done()
			rest.StartUIServer(ctx, addr)
		}()

		<-ch
		cancel()
		wg.Wait()
		log.Println("LogsGo shut down cleanly")
	},
}

func main() {
	rootCmd.Flags().StringVar(&cfg.DataDir, "data-dir", "data", "Data directory path to store logs data. Default value is ./data")
	rootCmd.Flags().StringVar(&cfg.MaxRetentionTime, "max-retention-time", "10d", "Maximum time blocks chunks remain on disk. Suffix with d/m/h/s")
	rootCmd.Flags().StringVar(&cfg.MaxTimeInMem, "max-time-in-mem", "1h", "Time logs remain in memory before persisting to disk. Suffix with d/m/h/s")
	rootCmd.Flags().BoolVar(&cfg.UnLockDataDir, "unlock-data-dir", false, "Allow other processes to access data directory (not recommended)")
	rootCmd.Flags().StringVar(&cfg.HttpListenAddr, "http-listen-addr", ":8080", "HTTP server listen address for REST API")
	rootCmd.Flags().StringVar(&cfg.GrpcListenAddr, "grpc-listen-addr", ":50051", "gRPC server listen address for ingestion")
	rootCmd.Flags().BoolVar(&cfg.FlushOnExit, "flush-on-exit", false, "If set on exit under any condition, logs will be flushed to its next store, if you want complete persistance")
	rootCmd.Flags().StringVar(&cfg.StoreConfigPath, "store-config-path", "", "Path to your s3 compatible store config path, if any")
	rootCmd.Flags().StringVar(&cfg.StoreConfig, "store-config", "", "s3 compatible store configuration, can't be used with --store-config-path flag")
	rootCmd.Flags().StringVar(&cfg.WebListenAddr, "web-listen-addr", "http://localhost:19091", "LogsGo web client address")
	rootCmd.Flags().StringVar(&cfg.LookbackPeriod, "lookback-period", "15m", "For instant queries (querying at current time) how much to look back in time from current time to fetch logs")
	rootCmd.Flags().SortFlags = true

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
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

func getAddr(httpAddr string) string {
	if strings.HasPrefix(httpAddr, "http://") {
		return strings.TrimPrefix(httpAddr, "http://")
	}

	if strings.HasPrefix(httpAddr, "http://") {
		return strings.TrimPrefix(httpAddr, "https://")
	}

	return httpAddr
}
