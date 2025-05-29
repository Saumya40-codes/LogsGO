package rest

import (
	"log"
	"net/http"
	"time"

	"github.com/Saumya40-codes/LogsGO/internal/ingestion"
	"github.com/Saumya40-codes/LogsGO/pkg"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

func StartServer(logServer *ingestion.LogIngestorServer, cfg *pkg.IngestionFactory) {
	r := gin.Default()
	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"http://localhost:3000", "http://localhost:5173"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	r.GET("/query", func(g *gin.Context) {
		expr := g.Query("expression")

		filter := ingestion.LogFilter{ // TODO: have some query parser to parse the expression, for now match expr to service name
			Service: expr,
		}

		results, err := logServer.QueryLogs(filter)
		if err != nil {
			g.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		g.JSON(http.StatusOK, results)
	})

	log.Println("Starting HTTP server on port :8080")

	r.Run(cfg.HttpListenAddr)
}
