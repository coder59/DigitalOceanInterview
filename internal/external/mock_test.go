package external_test

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"go-ingestion-api/internal/external"
)

func TestMockClientReturns429(t *testing.T) {
	c := external.NewMockClient(2, time.Second)
	ctx := context.Background()

	if _, err := c.Process(ctx, "a"); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if _, err := c.Process(ctx, "b"); err != nil {
		t.Fatalf("second call: %v", err)
	}
	_, err := c.Process(ctx, "c")
	if err == nil {
		t.Fatal("expected 429 on third call")
	}
	var rateErr *external.RateLimitError
	if !errors.As(err, &rateErr) {
		t.Fatalf("expected RateLimitError, got %T %v", err, err)
	}
	if !errors.Is(err, external.ErrRateLimited) {
		t.Fatal("expected errors.Is ErrRateLimited")
	}
	if rateErr.RetryAfter <= 0 {
		t.Fatalf("expected positive RetryAfter, got %s", rateErr.RetryAfter)
	}
}

func TestMockClientEncodes(t *testing.T) {
	c := external.NewMockClient(10, time.Second)
	got, err := c.Process(context.Background(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	want := base64.StdEncoding.EncodeToString([]byte("hello"))
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestMockClientRecoversAfterWindow(t *testing.T) {
	c := external.NewMockClient(1, 50*time.Millisecond)
	ctx := context.Background()

	if _, err := c.Process(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Process(ctx, "b"); err == nil {
		t.Fatal("expected 429 while window full")
	}

	time.Sleep(60 * time.Millisecond)
	got, err := c.Process(ctx, "c")
	if err != nil {
		t.Fatalf("expected success after window reset: %v", err)
	}
	want := base64.StdEncoding.EncodeToString([]byte("c"))
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}
