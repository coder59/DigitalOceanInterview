package plane_test

import (
	"context"
	"testing"
	"time"

	"go-ingestion-api/internal/plane"
)

func TestMemoryQueueDispatch(t *testing.T) {
	q := plane.NewMemoryQueue(2)
	ctx := context.Background()

	if err := q.Dispatch(ctx, plane.WorkItem{ID: "1", Prompt: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := q.Dispatch(ctx, plane.WorkItem{ID: "2", Prompt: "b"}); err != nil {
		t.Fatal(err)
	}
	if q.Len() != 2 {
		t.Fatalf("len=%d", q.Len())
	}

	item := <-q.Jobs()
	if item.ID != "1" {
		t.Fatalf("got %#v", item)
	}
}

func TestMemoryQueueDispatchCancel(t *testing.T) {
	q := plane.NewMemoryQueue(1)
	_ = q.Dispatch(context.Background(), plane.WorkItem{ID: "1", Prompt: "a"})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := q.Dispatch(ctx, plane.WorkItem{ID: "2", Prompt: "b"})
	if err == nil {
		t.Fatal("expected context error when queue full")
	}
}
