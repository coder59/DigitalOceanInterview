package dataplane

import (
	"context"
	"log"
	"time"

	"go-ingestion-api/internal/external"
	"go-ingestion-api/internal/plane"
)

const (
	defaultRetrySleep = 100 * time.Millisecond
	maxRetrySleep     = 2 * time.Second
)

func (e *Engine) scheduleRetry(item plane.WorkItem, delay time.Duration, cause error) {
	if delay < defaultRetrySleep {
		delay = defaultRetrySleep
	}
	if delay > maxRetrySleep {
		delay = maxRetrySleep
	}
	item.Attempts++
	item.RetryAt = time.Now().Add(delay)
	log.Printf("dp: prompt id=%s batch=%s prompt=%q → retry queue (attempt %d) sleep %s cause=%v",
		item.ID, item.BatchID, truncatePrompt(item.Prompt, 120), item.Attempts, delay, cause)

	select {
	case e.retryQueue <- item:
	default:
		e.retryQueue <- item
	}
}

func (e *Engine) runRetryDispatcher(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			e.drainRetryQueue()
			return
		case item, ok := <-e.retryQueue:
			if !ok {
				return
			}
			e.sleepThenRequeue(ctx, item)
		}
	}
}

func (e *Engine) sleepThenRequeue(ctx context.Context, item plane.WorkItem) {
	wait := time.Until(item.RetryAt)
	if wait > 0 {
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
		}
	}
	e.queue.Send(item)
}

func (e *Engine) drainRetryQueue() {
	for {
		select {
		case item := <-e.retryQueue:
			e.queue.Send(item)
		default:
			return
		}
	}
}

func retryDelay(attempts int, rateErr *external.RateLimitError) time.Duration {
	delay := defaultRetrySleep
	for i := 1; i < attempts; i++ {
		delay *= 2
		if delay >= maxRetrySleep {
			delay = maxRetrySleep
			break
		}
	}
	if rateErr != nil && rateErr.RetryAfter > delay {
		return rateErr.RetryAfter
	}
	return delay
}
