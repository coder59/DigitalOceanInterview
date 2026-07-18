package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go-ingestion-api/internal/batch"
	"go-ingestion-api/internal/controlplane"
	"go-ingestion-api/internal/dataplane"
	"go-ingestion-api/internal/db"
	"go-ingestion-api/internal/external"
	"go-ingestion-api/internal/plane"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/gin-gonic/gin"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func TestE2EIngestProcessAndCompile(t *testing.T) {
	pgPort := freePort(t)
	apiPort := freePort(t)

	runtimePath := filepath.Join(t.TempDir(), "pg")
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Username("postgres").
			Password("securepassword").
			Database("ingest_db").
			Version(embeddedpostgres.V16).
			Port(uint32(pgPort)).
			RuntimePath(runtimePath).
			StartTimeout(60 * time.Second),
	)
	if err := postgres.Start(); err != nil {
		t.Fatalf("start embedded postgres: %v", err)
	}
	defer func() { _ = postgres.Stop() }()

	connStr := fmt.Sprintf(
		"postgres://postgres:securepassword@127.0.0.1:%d/ingest_db?sslmode=disable",
		pgPort,
	)
	repo, err := db.InitDB(connStr)
	if err != nil {
		t.Fatalf("init db: %v", err)
	}
	sqlDB, err := repo.DB().DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()

	tracker := batch.NewTracker()
	workQueue := plane.NewMemoryQueue(1000)
	save := func(items []plane.WorkItem) error {
		err := repo.SaveSuccessfulInferences(items)
		counts := make(map[string]int, 4)
		for _, p := range items {
			if p.BatchID != "" {
				counts[p.BatchID]++
			}
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

	// Generous mock limits so e2e completes quickly; still exercises DP path.
	mockAPI := external.NewMockClient(50, time.Second)
	dp := dataplane.NewEngine(workQueue, dataplane.Config{
		MaxWorkers:     3,
		BatchSize:      10,
		FlushInterval:  200 * time.Millisecond,
		RetryQueueSize: 1000,
		Limiter:        dataplane.NewLeakyBucket(20, 50),
		API:            mockAPI,
		Saver:          save,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dp.Start(ctx)

	gin.SetMode(gin.ReleaseMode)
	cp := controlplane.NewServer(workQueue, dp, tracker, func(batchID string) (any, error) {
		return repo.GetCompiledInferences(batchID)
	})
	srv := &http.Server{Addr: fmt.Sprintf("127.0.0.1:%d", apiPort), Handler: cp.Router()}
	go func() { _ = srv.ListenAndServe() }()
	defer func() {
		shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = srv.Shutdown(shutdownCtx)
	}()

	base := fmt.Sprintf("http://127.0.0.1:%d", apiPort)
	waitHTTP(t, base+"/health", 10*time.Second)

	// health
	resp := mustGET(t, base+"/health")
	if resp["status"] != "healthy" || resp["plane"] != "control" {
		t.Fatalf("health: %#v", resp)
	}

	// ingest prompts
	body := `[{"prompt":"hello"},{"prompt":"world"},"third prompt"]`
	ingest := mustPOST(t, base+"/api/v1/ingest", body)
	if ingest["status"] != "accepted" {
		t.Fatalf("ingest: %#v", ingest)
	}
	batchID, _ := ingest["batch_id"].(string)
	if batchID == "" {
		t.Fatal("missing batch_id")
	}
	t.Logf("batch_id=%s", batchID)

	// poll until compiled results have all inferences
	var results map[string]any
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		st := mustGET(t, base+"/api/v1/ingest/batches/"+batchID)
		t.Logf("batch status: processed=%v total=%v status=%v", st["processed"], st["total"], st["status"])
		res, code, err := tryGET(base + "/api/v1/ingest/batches/" + batchID + "/results")
		if err == nil {
			count, _ := res["count"].(float64)
			if int(count) >= 3 {
				results = res
				break
			}
			t.Logf("results count=%v (waiting for 3)", count)
		} else {
			t.Logf("results not ready yet (status=%d): %v", code, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if results == nil {
		t.Fatal("compiled results not ready in time")
	}

	inferences, ok := results["inferences"].([]any)
	if !ok || len(inferences) != 3 {
		t.Fatalf("expected 3 inferences, got %#v", results["inferences"])
	}

	pool := mustGET(t, base+"/api/v1/pool")
	if pool["plane"] != "data" {
		t.Fatalf("pool stats: %#v", pool)
	}

	t.Logf("e2e OK: compiled %v inferences", results["count"])
}

func waitHTTP(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server not ready: %s", url)
}

func mustGET(t *testing.T, url string) map[string]any {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		t.Fatalf("GET %s -> %d %s", url, resp.StatusCode, b)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func tryGET(url string) (map[string]any, int, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return nil, resp.StatusCode, fmt.Errorf("status %d: %s", resp.StatusCode, b)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, resp.StatusCode, err
	}
	return out, resp.StatusCode, nil
}

func mustPOST(t *testing.T, url, body string) map[string]any {
	t.Helper()
	resp, err := http.Post(url, "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST %s -> %d %s", url, resp.StatusCode, b)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
