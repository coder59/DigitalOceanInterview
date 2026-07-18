package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go-ingestion-api/internal/batch"
	"go-ingestion-api/internal/controlplane"
	"go-ingestion-api/internal/dataplane"
	"go-ingestion-api/internal/db"
	"go-ingestion-api/internal/external"
	"go-ingestion-api/internal/plane"

	"github.com/gin-gonic/gin"
)

func main() {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "postgres://postgres:securepassword@localhost:5432/ingest_db?sslmode=disable"
	}

	repo, err := db.InitDB(connStr)
	if err != nil {
		log.Fatalf("failed to init db: %v", err)
	}
	sqlDB, err := repo.DB().DB()
	if err != nil {
		log.Fatalf("failed to get sql db: %v", err)
	}
	defer sqlDB.Close()

	tracker := batch.NewTracker()

	// Shared queue is the CP→DP boundary.
	workQueue := plane.NewMemoryQueue(10000)

	save := func(items []plane.WorkItem) error {
		err := repo.SaveSuccessfulInferences(items)
		counts := make(map[string]int, 4)
		for _, p := range items {
			if p.BatchID == "" {
				continue
			}
			counts[p.BatchID]++
		}
		if err != nil {
			for batchID, n := range counts {
				tracker.MarkFailed(batchID, n, err)
			}
			return err
		}
		for batchID, n := range counts {
			tracker.MarkProcessed(batchID, n)
		}
		return nil
	}

	// --- Data plane: does inference, 429 retry, persistence ---
	mockAPI := external.NewMockClient(5, time.Second)
	limiter := dataplane.NewLeakyBucket(8, 4)
	dp := dataplane.NewEngine(workQueue, dataplane.Config{
		MaxWorkers:     5,
		BatchSize:      100,
		FlushInterval:  2 * time.Second,
		RetryQueueSize: 10000,
		Limiter:        limiter,
		API:            mockAPI,
		Saver:          save,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dp.Start(ctx)
	log.Println("data plane started")

	// --- Control plane: validates, tracks, dispatches only ---
	cp := controlplane.NewServer(
		workQueue, // Dispatcher
		dp,        // StatsProvider
		tracker,
		func(batchID string) (any, error) {
			return repo.GetCompiledInferences(batchID)
		},
	)

	gin.SetMode(gin.ReleaseMode)
	srv := &http.Server{
		Addr:    ":8080",
		Handler: cp.Router(),
	}

	go func() {
		log.Println("control plane listening on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}
}
