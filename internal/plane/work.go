package plane

import (
	"context"
	"time"
)

// WorkItem is the unit of work CP dispatches and DP executes.
type WorkItem struct {
	ID        string    `json:"id,omitempty"`
	Prompt    string    `json:"prompt"`
	BatchID   string    `json:"batch_id,omitempty"`
	Attempts  int       `json:"-"`
	RetryAt   time.Time `json:"-"`
	Inference string    `json:"-"` // filled by data plane on success
}

// Dispatcher is the control-plane → data-plane boundary: CP only dispatches work.
type Dispatcher interface {
	Dispatch(ctx context.Context, item WorkItem) error
}

// StatsProvider exposes data-plane queue depths for CP status APIs.
type StatsProvider interface {
	Stats() (jobLen, jobCap, retryLen, workers int)
}

// MemoryQueue is an in-process work queue shared by CP (producer) and DP (consumer).
type MemoryQueue struct {
	jobs chan WorkItem
}

// NewMemoryQueue creates a buffered dispatch queue.
func NewMemoryQueue(size int) *MemoryQueue {
	if size < 1 {
		size = 1
	}
	return &MemoryQueue{jobs: make(chan WorkItem, size)}
}

// Dispatch enqueues work for the data plane. Blocks if the queue is full (never drops).
func (q *MemoryQueue) Dispatch(ctx context.Context, item WorkItem) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case q.jobs <- item:
		return nil
	}
}

// Jobs is consumed by the data plane.
func (q *MemoryQueue) Jobs() <-chan WorkItem {
	return q.jobs
}

// Send re-enqueues an item onto the job channel (used by DP retry path). Never drops.
func (q *MemoryQueue) Send(item WorkItem) {
	select {
	case q.jobs <- item:
	default:
		q.jobs <- item
	}
}

// Len returns current job queue depth.
func (q *MemoryQueue) Len() int { return len(q.jobs) }

// Cap returns job queue capacity.
func (q *MemoryQueue) Cap() int { return cap(q.jobs) }
