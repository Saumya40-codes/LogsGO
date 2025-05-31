package rest

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/Saumya40-codes/LogsGO/internal/ingestion"
	"github.com/Saumya40-codes/LogsGO/pkg"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

func StartServer(ctx context.Context, logServer *ingestion.LogIngestorServer, cfg *pkg.IngestionFactory) {
	r := gin.Default()
	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"http://localhost:3000", "http://localhost:5173"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	srv := &http.Server{
		Addr:    cfg.HttpListenAddr,
		Handler: r,
	}

	r.GET("/query", func(g *gin.Context) {
		expr := g.Query("expression")

		// filter := ingestion.LogFilter{ // TODO: have some query parser to parse the expression, for now match expr to service name
		// 	Service: expr,
		// }

		// // results, err := logServer.QueryLogs(filter)
		// // if err != nil {
		// // 	g.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		// // 	return
		// // }

		g.JSON(http.StatusOK, expr) // For now just return the expression, later we will implement the query logic
	})

	r.GET("/labels", func(g *gin.Context) {
		labels, err := logServer.Store.LabelValues()
		fmt.Println("Labels fetched:", labels)
		if err != nil {
			log.Printf("Error fetching labels: %v", err)
			g.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch labels"})
			return
		}

		g.JSON(http.StatusOK, labels)
	})

	log.Println("Starting HTTP server on port :8080")

	// Start the server
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe failed: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("Shutting down HTTP server...")
	if err := srv.Shutdown(context.Background()); err != nil {
		log.Printf("HTTP server shutdown failed: %v", err)
	} else {
		log.Println("HTTP server shut down gracefully")
	}
	log.Println("Stopping LogIngestorServer...")
}
