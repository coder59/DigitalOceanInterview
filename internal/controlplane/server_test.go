package controlplane_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go-ingestion-api/internal/batch"
	"go-ingestion-api/internal/controlplane"
	"go-ingestion-api/internal/plane"

	"github.com/gin-gonic/gin"
)

type recordingDispatcher struct {
	items []plane.WorkItem
}

func (d *recordingDispatcher) Dispatch(_ context.Context, item plane.WorkItem) error {
	d.items = append(d.items, item)
	return nil
}

type fakeStats struct{}

func (fakeStats) Stats() (int, int, int, int) { return 0, 100, 0, 5 }

func TestControlPlaneIngestDispatchesOnly(t *testing.T) {
	gin.SetMode(gin.TestMode)
	disp := &recordingDispatcher{}
	tracker := batch.NewTracker()
	srv := controlplane.NewServer(disp, fakeStats{}, tracker, nil)

	body := `[{"prompt":"hello"},{"prompt":"world"}]`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["plane"] != "control" {
		t.Fatalf("expected control plane ack, got %v", resp)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(disp.items) == 2 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected CP to dispatch 2 items, got %d", len(disp.items))
}

func TestControlPlaneRejectsEmptyPrompt(t *testing.T) {
	gin.SetMode(gin.TestMode)
	srv := controlplane.NewServer(&recordingDispatcher{}, fakeStats{}, batch.NewTracker(), nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest", bytes.NewBufferString(`[{"prompt":""}]`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}
