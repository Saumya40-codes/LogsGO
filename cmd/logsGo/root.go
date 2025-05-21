package main

import (
	"log"

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
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
