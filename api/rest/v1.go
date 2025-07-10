package rest

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/Saumya40-codes/LogsGO/api/auth"
	"github.com/Saumya40-codes/LogsGO/internal/ingestion"
	"github.com/Saumya40-codes/LogsGO/pkg"
	"github.com/Saumya40-codes/LogsGO/pkg/store"
	"github.com/Saumya40-codes/LogsGO/pkg/ui"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

type LabelValuesResponse struct {
	Services []string
	Levels   []string
}

func StartServer(ctx context.Context, logServer *ingestion.LogIngestorServer, cfg *pkg.IngestionFactory, authCfg auth.AuthConfig) {
	r := gin.Default()

	allowedOrigins := []string{"http://localhost:5173"}

	var origin string
	if !strings.HasPrefix(cfg.WebListenAddr, "http://") && !strings.HasPrefix(cfg.WebListenAddr, "https://") {
		addr := strings.Split(cfg.WebListenAddr, ":")
		var host, port string
		if len(addr) == 2 {
			host = addr[0]
			port = addr[1]
		} else if len(addr) == 1 {
			host = addr[0]
			port = "80"
		} else {
			log.Printf("unexpected WebListenAddr format: %s", cfg.WebListenAddr)
		}

		if host == "0.0.0.0" || host == "" {
			host = "localhost"
		}

		origin = "http://" + host + ":" + port
	} else {
		origin = cfg.WebListenAddr
	}
	allowedOrigins = append(allowedOrigins, origin)

	r.Use(cors.New(cors.Config{
		AllowOrigins:     allowedOrigins,
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
	}))

	middlewares := make([]gin.HandlerFunc, 0)

	if authCfg.PublicKeyPath != "" && authCfg.PublicKey != nil {
		middlewares = append(middlewares, auth.JwtMiddleware(authCfg.PublicKey))
	}

	api := r.Group("/api/v1")
	api.Use(middlewares...)
	{
		api.GET("/query", func(g *gin.Context) {
			expr := g.Query("expression")
			startTs := g.Query("start")
			endTs := g.Query("end")
			resolution := g.Query("resolution")

			if !pkg.ValidateTimeDurations(resolution) {
				log.Printf("Invalid resolution format: %s", resolution)
				g.JSON(http.StatusBadRequest, gin.H{"error": "Invalid resolution format"})
				return
			}

			qReq := ingestion.QueryRequest{
				Query:      expr,
				Resolution: int64(pkg.GetTimeDuration(resolution).Seconds()),
			}

			sTs, eTs, err := parserTimestamp(startTs, endTs)
			if err != nil {
				log.Printf("Error parsing timestamps: %v", err)
				g.JSON(http.StatusBadRequest, gin.H{"error": "Invalid timestamp format"})
				return
			}

			qReq.StartTs = sTs
			qReq.EndTs = eTs

			res, err := logServer.MakeQuery(qReq)
			if err != nil {
				log.Printf("Error processing query: %v", err)
				g.JSON(http.StatusBadRequest, gin.H{"error": "Invalid query expression"})
				return
			}
			g.JSON(http.StatusOK, res)
		})

		api.GET("/labels", func(g *gin.Context) {
			labels := &store.Labels{
				Services: make(map[string]int),
				Levels:   make(map[string]int),
			}
			err := logServer.Store.LabelValues(labels)

			respLabels := ConvertToResponse(*labels)
			if err != nil {
				log.Printf("Error fetching labels: %v", err)
				g.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch labels"})
				return
			}

			g.JSON(http.StatusOK, respLabels)
		})

		// health e.p. liveness
		api.GET("/healthz", func(g *gin.Context) {
			g.Status(http.StatusOK)
		})

		// readiness
		api.GET("/readyz", func(g *gin.Context) {
			if !logServer.IsReady() {
				g.JSON(http.StatusServiceUnavailable, gin.H{"error": "not ready"})
				return
			}
			g.Status(http.StatusOK)
		})
	}

	srv := &http.Server{
		Addr:    cfg.HttpListenAddr,
		Handler: r,
	}

	log.Printf("Starting HTTP server on %s\n", cfg.HttpListenAddr)

	go func() {
		var servErr error

		if authCfg.TLSCfg != nil && authCfg.TLSCfg.HttpClient.Config != nil && authCfg.TLSCfg.HttpClient.Config.Enabled {
			if _, err := os.Stat(authCfg.TLSCfg.HttpClient.Config.CertFile); os.IsNotExist(err) {
				log.Fatalf("TLS cert not found: %s", authCfg.TLSCfg.HttpClient.Config.CertFile)
			}
			log.Printf("Using TLS cert: %s", authCfg.TLSCfg.HttpClient.Config.CertFile)

			servErr = srv.ListenAndServeTLS(authCfg.TLSCfg.HttpClient.Config.CertFile, authCfg.TLSCfg.HttpClient.Config.KeyFile)
		} else {
			servErr = srv.ListenAndServe()
		}
		if servErr != nil && servErr != http.ErrServerClosed {
			log.Fatalf("ListenAndServe failed: %v", servErr)
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

func StartUIServer(ctx context.Context, webListenAddr string) {
	r := gin.Default()

	r.NoRoute(ui.Handler())

	srv := &http.Server{
		Addr:    webListenAddr,
		Handler: r,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("UI server failed: %v", err)
		}
	}()

	log.Printf("UI server started at %v\n", webListenAddr)
	<-ctx.Done()
	log.Println("Shutting down UI server...")
	if err := srv.Shutdown(context.Background()); err != nil {
		log.Printf("UI server shutdown failed: %v", err)
	}
}

func ConvertToResponse(labels store.Labels) LabelValuesResponse {
	resp := LabelValuesResponse{
		Services: make([]string, 0),
		Levels:   make([]string, 0),
	}

	for service := range labels.Services {
		resp.Services = append(resp.Services, service)
	}

	for level := range labels.Levels {
		resp.Levels = append(resp.Levels, level)
	}

	return resp
}

func parserTimestamp(startTs, endTs string) (int64, int64, error) {
	start, err := strconv.ParseInt(startTs, 10, 64)
	if err != nil {
		return 0, 0, err
	}

	end, err := strconv.ParseInt(endTs, 10, 64)
	if err != nil {
		return 0, 0, err
	}

	return start, end, nil
}
