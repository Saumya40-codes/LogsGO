package rest

import (
	"fmt"
	"log"
	"net/http"

	"github.com/Saumya40-codes/LogsGO/internal/ingestion"
	"github.com/gin-gonic/gin"
)

func StartServer(logServer *ingestion.LogIngestorServer) {
	r := gin.Default()

	r.GET("/query", func(g *gin.Context) {
		level := g.Query("level")
		service := g.Query("service")

		fmt.Println(service)

		filter := ingestion.LogFilter{
			Level:   level,
			Service: service,
		}

		results, err := logServer.QueryLogs(filter)
		if err != nil {
			g.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		g.JSON(http.StatusOK, results)
	})

	log.Println("Starting HTTP server on port :8080")

	r.Run(":8080")
}
