package external

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrRateLimited is returned when the mock API responds with HTTP 429 semantics.
var ErrRateLimited = errors.New("429 too many requests")

// RateLimitError carries an optional retry-after hint from the mock API.
type RateLimitError struct {
	RetryAfter time.Duration
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("429 too many requests (retry after %s)", e.RetryAfter)
}

func (e *RateLimitError) Is(target error) bool {
	return target == ErrRateLimited
}

// MockClient simulates an external encoding API with a fixed-window rate limit.
// Requests beyond limitPerWindow in window return 429.
type MockClient struct {
	mu             sync.Mutex
	limitPerWindow int
	window         time.Duration
	windowStart    time.Time
	count          int
}

// NewMockClient creates a rate-limited mock external API.
// Example: NewMockClient(5, time.Second) allows 5 calls/second, then 429.
func NewMockClient(limitPerWindow int, window time.Duration) *MockClient {
	if limitPerWindow < 1 {
		limitPerWindow = 1
	}
	if window <= 0 {
		window = time.Second
	}
	return &MockClient{
		limitPerWindow: limitPerWindow,
		window:         window,
		windowStart:    time.Now(),
	}
}

// Process base64-encodes the prompt, or returns a 429 rate-limit error.
func (c *MockClient) Process(ctx context.Context, prompt string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	c.mu.Lock()
	now := time.Now()
	if now.Sub(c.windowStart) >= c.window {
		c.windowStart = now
		c.count = 0
	}
	if c.count >= c.limitPerWindow {
		retryAfter := c.window - now.Sub(c.windowStart)
		if retryAfter < 10*time.Millisecond {
			retryAfter = 10 * time.Millisecond
		}
		c.mu.Unlock()
		return "", &RateLimitError{RetryAfter: retryAfter}
	}
	c.count++
	c.mu.Unlock()

	// Simulate a small amount of external work.
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(2 * time.Millisecond):
	}

	return base64.StdEncoding.EncodeToString([]byte(prompt)), nil
}
