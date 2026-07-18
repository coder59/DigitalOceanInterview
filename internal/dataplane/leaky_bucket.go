package dataplane

import (
	"context"
	"sync"
	"time"
)

// LeakyBucket paces outbound calls: tokens refill at leakRate per second up to capacity.
type LeakyBucket struct {
	capacity float64
	leakRate float64
	mu       sync.Mutex
	tokens   float64
	last     time.Time
}

// NewLeakyBucket creates a leaky bucket with the given burst capacity and steady leak rate.
func NewLeakyBucket(capacity int, leakRatePerSec float64) *LeakyBucket {
	if capacity < 1 {
		capacity = 1
	}
	if leakRatePerSec <= 0 {
		leakRatePerSec = 1
	}
	return &LeakyBucket{
		capacity: float64(capacity),
		leakRate: leakRatePerSec,
		tokens:   float64(capacity),
		last:     time.Now(),
	}
}

func (b *LeakyBucket) refillLocked(now time.Time) {
	elapsed := now.Sub(b.last).Seconds()
	if elapsed <= 0 {
		return
	}
	b.tokens += elapsed * b.leakRate
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}
	b.last = now
}

// Wait blocks until one token is available or ctx is cancelled.
func (b *LeakyBucket) Wait(ctx context.Context) error {
	for {
		b.mu.Lock()
		now := time.Now()
		b.refillLocked(now)
		if b.tokens >= 1 {
			b.tokens--
			b.mu.Unlock()
			return nil
		}
		need := 1 - b.tokens
		wait := time.Duration(need / b.leakRate * float64(time.Second))
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
		b.mu.Unlock()

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
