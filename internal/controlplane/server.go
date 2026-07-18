package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go-ingestion-api/internal/batch"
	"go-ingestion-api/internal/plane"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const maxBatchSize = 1000

type promptsEnvelope struct {
	Prompts []json.RawMessage `json:"prompts"`
}

type promptItem struct {
	ID     string `json:"id"`
	Prompt string `json:"prompt"`
}

// Server is the control plane: validates requests, tracks batches, dispatches work to the data plane.
type Server struct {
	dispatcher plane.Dispatcher
	stats      plane.StatsProvider
	tracker    *batch.Tracker
	results    func(batchID string) (any, error)
}

// NewServer constructs the control-plane HTTP API.
func NewServer(
	dispatcher plane.Dispatcher,
	stats plane.StatsProvider,
	tracker *batch.Tracker,
	results func(batchID string) (any, error),
) *Server {
	return &Server{
		dispatcher: dispatcher,
		stats:      stats,
		tracker:    tracker,
		results:    results,
	}
}

// Router returns the Gin engine with control-plane routes.
func (s *Server) Router() *gin.Engine {
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "healthy", "plane": "control"})
	})
	r.POST("/api/v1/ingest", s.handleIngest)
	r.GET("/api/v1/ingest/batches/:batch_id", s.handlePollBatch)
	r.GET("/api/v1/ingest/batches/:batch_id/results", s.handleCompiledResults)
	r.GET("/api/v1/pool", s.handlePoolStats)
	return r
}

func (s *Server) handleIngest(c *gin.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read body"})
		return
	}

	items, err := parsePrompts(body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if len(items) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "prompts array must contain at least 1 item"})
		return
	}
	if len(items) > maxBatchSize {
		c.JSON(http.StatusBadRequest, gin.H{"error": "prompts array must contain at most 1000 items"})
		return
	}

	batchID := s.tracker.Create(len(items))
	// Ack immediately; dispatch is asynchronous — CP does not run inference.
	go s.dispatchBatch(batchID, items)

	c.JSON(http.StatusAccepted, gin.H{
		"status":   "accepted",
		"batch_id": batchID,
		"total":    len(items),
		"plane":    "control",
	})
}

func (s *Server) dispatchBatch(batchID string, items []plane.WorkItem) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	for i := range items {
		items[i].BatchID = batchID
		if err := s.dispatcher.Dispatch(ctx, items[i]); err != nil {
			remaining := len(items) - i
			s.tracker.MarkFailed(batchID, remaining, err)
			return
		}
		s.tracker.MarkQueued(batchID, 1)
	}
}

func (s *Server) handlePollBatch(c *gin.Context) {
	info, ok := s.tracker.Get(c.Param("batch_id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "batch not found"})
		return
	}
	c.JSON(http.StatusOK, info)
}

func (s *Server) handleCompiledResults(c *gin.Context) {
	if s.results == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "compiled inferences not found"})
		return
	}
	doc, err := s.results(c.Param("batch_id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "compiled inferences not found"})
		return
	}
	c.JSON(http.StatusOK, doc)
}

func (s *Server) handlePoolStats(c *gin.Context) {
	jobLen, jobCap, retryLen, workers := s.stats.Stats()
	c.JSON(http.StatusOK, gin.H{
		"plane":              "data",
		"queue_length":       jobLen,
		"queue_cap":          jobCap,
		"retry_queue_length": retryLen,
		"max_workers":        workers,
	})
}

func parsePrompts(body []byte) ([]plane.WorkItem, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("empty body")
	}

	var rawItems []json.RawMessage
	switch trimmed[0] {
	case '[':
		if err := json.Unmarshal(trimmed, &rawItems); err != nil {
			return nil, fmt.Errorf("invalid json array: %w", err)
		}
	case '{':
		var env promptsEnvelope
		if err := json.Unmarshal(trimmed, &env); err != nil {
			return nil, fmt.Errorf("invalid json object: %w", err)
		}
		if env.Prompts == nil {
			return nil, fmt.Errorf(`body must be a json array of prompts or {"prompts":[...]}`)
		}
		rawItems = env.Prompts
	default:
		return nil, fmt.Errorf(`body must be a json array of prompts or {"prompts":[...]}`)
	}

	out := make([]plane.WorkItem, 0, len(rawItems))
	for i, raw := range rawItems {
		prompt, id, err := decodePromptElement(raw)
		if err != nil {
			return nil, fmt.Errorf("item[%d]: %w", i, err)
		}
		if strings.TrimSpace(prompt) == "" {
			return nil, fmt.Errorf("item[%d]: prompt is required", i)
		}
		if id == "" {
			id = uuid.NewString()
		} else if _, err := uuid.Parse(id); err != nil {
			return nil, fmt.Errorf("item[%d]: id must be a valid uuid", i)
		}
		out = append(out, plane.WorkItem{ID: id, Prompt: prompt})
	}
	return out, nil
}

func decodePromptElement(raw json.RawMessage) (prompt, id string, err error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return "", "", fmt.Errorf("empty item")
	}
	if trimmed[0] == '"' {
		if err := json.Unmarshal(trimmed, &prompt); err != nil {
			return "", "", fmt.Errorf("invalid prompt string: %w", err)
		}
		return prompt, "", nil
	}
	var item promptItem
	if err := json.Unmarshal(trimmed, &item); err != nil {
		return "", "", fmt.Errorf("invalid prompt object: %w", err)
	}
	return item.Prompt, item.ID, nil
}
