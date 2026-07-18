package dataplane

import (
	"context"
	"errors"
	"log"
	"time"

	"go-ingestion-api/internal/external"
	"go-ingestion-api/internal/plane"
)

// PromptAPI is the external inference service (mock returns 429 when overloaded).
type PromptAPI interface {
	Process(ctx context.Context, prompt string) (string, error)
}

// Engine is the data plane: consumes dispatched work, rate-limits, retries, persists.
type Engine struct {
	queue         *plane.MemoryQueue
	retryQueue    chan plane.WorkItem
	maxWorkers    int
	batchSize     int
	flushInterval time.Duration
	limiter       *LeakyBucket
	api           PromptAPI
	saver         func([]plane.WorkItem) error
}

// Config configures the data-plane engine.
type Config struct {
	MaxWorkers     int
	BatchSize      int
	FlushInterval  time.Duration
	RetryQueueSize int
	Limiter        *LeakyBucket
	API            PromptAPI
	Saver          func([]plane.WorkItem) error
}

// NewEngine creates a data-plane engine bound to the shared work queue from CP.
func NewEngine(queue *plane.MemoryQueue, cfg Config) *Engine {
	if cfg.MaxWorkers < 1 {
		cfg.MaxWorkers = 1
	}
	if cfg.BatchSize < 1 {
		cfg.BatchSize = 1
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 2 * time.Second
	}
	retrySize := cfg.RetryQueueSize
	if retrySize < 1000 {
		retrySize = 1000
	}
	return &Engine{
		queue:         queue,
		retryQueue:    make(chan plane.WorkItem, retrySize),
		maxWorkers:    cfg.MaxWorkers,
		batchSize:     cfg.BatchSize,
		flushInterval: cfg.FlushInterval,
		limiter:       cfg.Limiter,
		api:           cfg.API,
		saver:         cfg.Saver,
	}
}

// Stats implements plane.StatsProvider for the control plane.
func (e *Engine) Stats() (jobLen, jobCap, retryLen, workers int) {
	return e.queue.Len(), e.queue.Cap(), len(e.retryQueue), e.maxWorkers
}

// Start runs the data-plane retry dispatcher and workers.
func (e *Engine) Start(ctx context.Context) {
	go e.runRetryDispatcher(ctx)
	for i := 0; i < e.maxWorkers; i++ {
		go e.runWorker(ctx)
	}
}

func (e *Engine) runWorker(ctx context.Context) {
	batch := make([]plane.WorkItem, 0, e.batchSize)
	ticker := time.NewTicker(e.flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		processed := e.processAll(context.Background(), batch)
		if len(processed) > 0 && e.saver != nil {
			_ = e.saver(processed)
		}
		batch = batch[:0]
	}

	jobs := e.queue.Jobs()
	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case item, ok := <-jobs:
			if !ok {
				flush()
				return
			}
			batch = append(batch, item)
			if len(batch) >= e.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (e *Engine) processAll(ctx context.Context, items []plane.WorkItem) []plane.WorkItem {
	out := make([]plane.WorkItem, 0, len(items))
	for _, item := range items {
		log.Printf("dp: processing prompt id=%s batch=%s attempts=%d prompt=%q",
			item.ID, item.BatchID, item.Attempts, truncatePrompt(item.Prompt, 120))

		if err := e.limiter.Wait(ctx); err != nil {
			e.scheduleRetry(item, defaultRetrySleep, err)
			continue
		}

		encoded, err := e.api.Process(ctx, item.Prompt)
		if err == nil {
			item.Inference = encoded
			log.Printf("dp: processed prompt id=%s batch=%s ok inference=%q",
				item.ID, item.BatchID, truncatePrompt(encoded, 64))
			out = append(out, item)
			continue
		}

		var rateErr *external.RateLimitError
		_ = errors.As(err, &rateErr)
		delay := retryDelay(item.Attempts+1, rateErr)
		e.scheduleRetry(item, delay, err)
	}
	return out
}

func truncatePrompt(s string, max int) string {
	if max < 1 || len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
