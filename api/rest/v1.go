package rest

import (
	"context"
	"log"
	"net/http"

	"github.com/Saumya40-codes/LogsGO/internal/ingestion"
	"github.com/Saumya40-codes/LogsGO/pkg"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

func StartServer(ctx context.Context, logServer *ingestion.LogIngestorServer, cfg *pkg.IngestionFactory) {
	r := gin.Default()

	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"http://localhost:3000", "http://localhost:5173"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
	}))

	api := r.Group("/api/v1")
	{
		api.GET("/query", func(g *gin.Context) {
			expr := g.Query("expression")
			res, err := logServer.MakeQuery(expr)
			if err != nil {
				log.Printf("Error processing query: %v", err)
				g.JSON(http.StatusBadRequest, gin.H{"error": "Invalid query expression"})
				return
			}
			g.JSON(http.StatusOK, res)
		})

		api.GET("/labels", func(g *gin.Context) {
			labels, err := logServer.Store.LabelValues()
			if err != nil {
				log.Printf("Error fetching labels: %v", err)
				g.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch labels"})
				return
			}

			g.JSON(http.StatusOK, labels)
		})
	}

	srv := &http.Server{
		Addr:    cfg.HttpListenAddr,
		Handler: r,
	}

	log.Printf("Starting HTTP server on %s\n", cfg.HttpListenAddr)

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
