package dataplane

import (
	"context"
	"encoding/base64"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go-ingestion-api/internal/external"
	"go-ingestion-api/internal/plane"
)

func TestRetryDelayExponentialBackoff(t *testing.T) {
	tests := []struct {
		name     string
		attempts int
		rateErr  *external.RateLimitError
		want     time.Duration
	}{
		{"first_attempt", 1, nil, 100 * time.Millisecond},
		{"second_attempt", 2, nil, 200 * time.Millisecond},
		{"third_attempt", 3, nil, 400 * time.Millisecond},
		{"fourth_attempt", 4, nil, 800 * time.Millisecond},
		{"fifth_attempt", 5, nil, 1600 * time.Millisecond},
		{"sixth_capped", 6, nil, 2 * time.Second},
		{"retry_after_wins", 1, &external.RateLimitError{RetryAfter: 500 * time.Millisecond}, 500 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := retryDelay(tt.attempts, tt.rateErr)
			if got != tt.want {
				t.Fatalf("retryDelay(%d) = %v, want %v", tt.attempts, got, tt.want)
			}
		})
	}
}

type stubAPI struct {
	mu           sync.Mutex
	calls        int
	failTimes    int
	retryAfter   time.Duration
	successCalls atomic.Int32
	rateLimited  atomic.Int32
}

func (s *stubAPI) Process(_ context.Context, prompt string) (string, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()
	if call <= s.failTimes {
		s.rateLimited.Add(1)
		return "", &external.RateLimitError{RetryAfter: s.retryAfter}
	}
	s.successCalls.Add(1)
	return base64.StdEncoding.EncodeToString([]byte(prompt)), nil
}

func newTestEngine(api PromptAPI, saver func([]plane.WorkItem) error) (*Engine, *plane.MemoryQueue) {
	q := plane.NewMemoryQueue(50)
	e := NewEngine(q, Config{
		MaxWorkers:     1,
		BatchSize:      10,
		FlushInterval:  time.Hour,
		RetryQueueSize: 50,
		Limiter:        NewLeakyBucket(20, 1000),
		API:            api,
		Saver:          saver,
	})
	return e, q
}

func TestProcessAllParksOn429WithBackoff(t *testing.T) {
	api := &stubAPI{failTimes: 1, retryAfter: 250 * time.Millisecond}
	e, _ := newTestEngine(api, nil)

	out := e.processAll(context.Background(), []plane.WorkItem{{ID: "id-1", Prompt: "hello"}})
	if len(out) != 0 {
		t.Fatalf("expected no success, got %d", len(out))
	}
	select {
	case parked := <-e.retryQueue:
		if parked.Attempts != 1 {
			t.Fatalf("Attempts=%d", parked.Attempts)
		}
		remaining := time.Until(parked.RetryAt)
		if remaining < 150*time.Millisecond || remaining > 300*time.Millisecond {
			t.Fatalf("RetryAt remaining=%s", remaining)
		}
	case <-time.After(time.Second):
		t.Fatal("expected retry queue item")
	}
}

func Test429BackoffEndToEnd(t *testing.T) {
	api := &stubAPI{failTimes: 3, retryAfter: 80 * time.Millisecond}
	var mu sync.Mutex
	var saved []plane.WorkItem
	e, q := newTestEngine(api, func(batch []plane.WorkItem) error {
		mu.Lock()
		defer mu.Unlock()
		saved = append(saved, batch...)
		return nil
	})
	e.batchSize = 1
	e.flushInterval = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)

	start := time.Now()
	_ = q.Dispatch(context.Background(), plane.WorkItem{ID: "e2e-1", Prompt: "backoff-me"})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(saved)
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	elapsed := time.Since(start)
	mu.Lock()
	defer mu.Unlock()
	if len(saved) != 1 {
		t.Fatalf("saved=%d rateLimited=%d", len(saved), api.rateLimited.Load())
	}
	if api.rateLimited.Load() < 3 {
		t.Fatalf("expected >=3 rate limits, got %d", api.rateLimited.Load())
	}
	if elapsed < 200*time.Millisecond {
		t.Fatalf("expected backoff delay, elapsed=%s", elapsed)
	}
	if saved[0].Attempts < 3 {
		t.Fatalf("Attempts=%d", saved[0].Attempts)
	}
}

func TestScheduleRetryNeverDrops(t *testing.T) {
	e, _ := newTestEngine(&stubAPI{}, nil)
	e.scheduleRetry(plane.WorkItem{ID: "a", Prompt: "p"}, 50*time.Millisecond, errors.New("429"))
	e.scheduleRetry(plane.WorkItem{ID: "b", Prompt: "q"}, 50*time.Millisecond, errors.New("429"))
	if len(e.retryQueue) != 2 {
		t.Fatalf("retry queue len=%d", len(e.retryQueue))
	}
}

func TestLeakyBucketPaces(t *testing.T) {
	b := NewLeakyBucket(1, 10)
	start := time.Now()
	_ = b.Wait(context.Background())
	_ = b.Wait(context.Background())
	if time.Since(start) < 80*time.Millisecond {
		t.Fatal("expected pacing delay")
	}
}

func TestControlPlaneDispatchReachesDataPlane(t *testing.T) {
	api := &stubAPI{failTimes: 0}
	var mu sync.Mutex
	var saved []plane.WorkItem
	e, q := newTestEngine(api, func(batch []plane.WorkItem) error {
		mu.Lock()
		saved = append(saved, batch...)
		mu.Unlock()
		return nil
	})
	e.batchSize = 1
	e.flushInterval = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)

	// Simulate CP dispatch only.
	if err := q.Dispatch(context.Background(), plane.WorkItem{ID: "cp-1", Prompt: "hi", BatchID: "b1"}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(saved)
		mu.Unlock()
		if n >= 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("data plane did not process CP-dispatched work")
}
