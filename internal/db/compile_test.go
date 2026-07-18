package db_test

import (
	"encoding/json"
	"testing"
	"time"

	"go-ingestion-api/internal/db"
	"go-ingestion-api/internal/plane"
)

func TestInferenceDocumentJSONShape(t *testing.T) {
	doc := db.InferenceDocument{
		BatchID: "batch-1",
		Count:   2,
		Inferences: []db.InferenceItem{
			{ID: "1", Prompt: "hello", Inference: "aGVsbG8=", Attempts: 0},
			{ID: "2", Prompt: "world", Inference: "d29ybGQ=", Attempts: 2},
		},
		UpdatedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	}

	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}

	var decoded db.InferenceDocument
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Count != 2 || len(decoded.Inferences) != 2 {
		t.Fatalf("unexpected decode: %+v", decoded)
	}
}

func TestCompileFromPayloadsShape(t *testing.T) {
	payloads := []plane.WorkItem{
		{ID: "a", Prompt: "p1", Inference: "eDE=", Attempts: 1, BatchID: "b1"},
		{ID: "b", Prompt: "p2", Inference: "eDI=", Attempts: 0, BatchID: "b1"},
	}

	items := make([]db.InferenceItem, 0, len(payloads))
	for _, p := range payloads {
		items = append(items, db.InferenceItem{
			ID:        p.ID,
			Prompt:    p.Prompt,
			Inference: p.Inference,
			Attempts:  p.Attempts,
		})
	}
	doc := db.InferenceDocument{
		BatchID:    "b1",
		Count:      len(items),
		Inferences: items,
		UpdatedAt:  time.Now().UTC(),
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(raw) || doc.Count != 2 {
		t.Fatalf("bad compile doc count=%d", doc.Count)
	}
}
