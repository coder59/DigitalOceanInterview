package batch

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

// Status represents the lifecycle of an ingest batch.
type Status string

const (
	StatusAccepted   Status = "accepted"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
)

// Info is a snapshot of batch progress for the polling API.
type Info struct {
	BatchID   string    `json:"batch_id"`
	Status    Status    `json:"status"`
	Total     int       `json:"total"`
	Queued    int       `json:"queued"`
	Processed int       `json:"processed"`
	Failed    int       `json:"failed"`
	Error     string    `json:"error,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type record struct {
	id        string
	status    Status
	total     int
	queued    int
	processed int
	failed    int
	errMsg    string
	createdAt time.Time
	updatedAt time.Time
}

// Tracker keeps in-memory progress for accepted ingest batches.
type Tracker struct {
	mu      sync.RWMutex
	batches map[string]*record
}

// NewTracker creates an empty batch tracker.
func NewTracker() *Tracker {
	return &Tracker{batches: make(map[string]*record)}
}

// Create registers a new batch and returns its ID.
func (t *Tracker) Create(total int) string {
	now := time.Now().UTC()
	id := uuid.NewString()
	t.mu.Lock()
	t.batches[id] = &record{
		id:        id,
		status:    StatusAccepted,
		total:     total,
		createdAt: now,
		updatedAt: now,
	}
	t.mu.Unlock()
	return id
}

// MarkQueued increments how many events were placed on the worker queue.
func (t *Tracker) MarkQueued(batchID string, n int) {
	if n <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	r, ok := t.batches[batchID]
	if !ok {
		return
	}
	r.queued += n
	r.status = StatusProcessing
	r.updatedAt = time.Now().UTC()
	t.refreshLocked(r)
}

// MarkProcessed increments successfully persisted events for a batch.
func (t *Tracker) MarkProcessed(batchID string, n int) {
	if n <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	r, ok := t.batches[batchID]
	if !ok {
		return
	}
	r.processed += n
	r.updatedAt = time.Now().UTC()
	t.refreshLocked(r)
}

// MarkFailed records failed events and an optional error message.
func (t *Tracker) MarkFailed(batchID string, n int, err error) {
	if n <= 0 && err == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	r, ok := t.batches[batchID]
	if !ok {
		return
	}
	if n > 0 {
		r.failed += n
	}
	if err != nil {
		r.errMsg = err.Error()
		r.status = StatusFailed
	}
	r.updatedAt = time.Now().UTC()
	t.refreshLocked(r)
}

func (t *Tracker) refreshLocked(r *record) {
	if r.status == StatusFailed {
		return
	}
	if r.processed+r.failed >= r.total && r.queued >= r.total {
		if r.failed > 0 {
			r.status = StatusFailed
		} else {
			r.status = StatusCompleted
		}
		return
	}
	if r.queued > 0 || r.processed > 0 {
		r.status = StatusProcessing
	}
}

// Get returns a copy of batch info, or false if unknown.
func (t *Tracker) Get(batchID string) (Info, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	r, ok := t.batches[batchID]
	if !ok {
		return Info{}, false
	}
	return Info{
		BatchID:   r.id,
		Status:    r.status,
		Total:     r.total,
		Queued:    r.queued,
		Processed: r.processed,
		Failed:    r.failed,
		Error:     r.errMsg,
		CreatedAt: r.createdAt,
		UpdatedAt: r.updatedAt,
	}, true
}
