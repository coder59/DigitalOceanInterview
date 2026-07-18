package db

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"go-ingestion-api/internal/plane"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

// InferenceItem is one successful mock-inference result in the compiled JSON.
type InferenceItem struct {
	ID        string `json:"id"`
	Prompt    string `json:"prompt"`
	Inference string `json:"inference"`
	Attempts  int    `json:"attempts"`
}

// InferenceDocument is the compiled JSON saved for a batch.
type InferenceDocument struct {
	BatchID    string          `json:"batch_id"`
	Count      int             `json:"count"`
	Inferences []InferenceItem `json:"inferences"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// GormPrompt maps to the prompts table (per-prompt row).
type GormPrompt struct {
	ID           string    `gorm:"column:id;type:uuid;primaryKey"`
	BatchID      string    `gorm:"column:batch_id;type:uuid;index"`
	Prompt       string    `gorm:"column:prompt;type:text;not null"`
	ProcessedB64 string    `gorm:"column:processed_b64;type:text;not null"`
	CreatedAt    time.Time `gorm:"column:created_at;type:timestamptz;default:current_timestamp"`
}

func (GormPrompt) TableName() string {
	return "prompts"
}

// GormInferenceCompilation stores the compiled JSON of all successful inferences for a batch.
type GormInferenceCompilation struct {
	BatchID    string    `gorm:"column:batch_id;type:uuid;primaryKey"`
	ResultJSON string    `gorm:"column:result_json;type:jsonb;not null"`
	Count      int       `gorm:"column:count;not null"`
	UpdatedAt  time.Time `gorm:"column:updated_at;type:timestamptz;default:current_timestamp"`
}

func (GormInferenceCompilation) TableName() string {
	return "inference_compilations"
}

// GormRepo wraps a GORM database connection.
type GormRepo struct {
	db    *gorm.DB
	write sync.Mutex // serializes compilation merges across data-plane workers
}

// InitDB opens a GORM connection, configures the pool, and runs AutoMigrate.
func InitDB(connStr string) (*GormRepo, error) {
	gdb, err := gorm.Open(postgres.Open(connStr), &gorm.Config{
		PrepareStmt: true,
		Logger:      logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, err
	}

	sqlDB, err := gdb.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(25)
	sqlDB.SetMaxIdleConns(5)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)

	if err := gdb.AutoMigrate(&GormPrompt{}, &GormInferenceCompilation{}); err != nil {
		return nil, err
	}

	return &GormRepo{db: gdb}, nil
}

// DB returns the underlying *gorm.DB for shutdown helpers.
func (r *GormRepo) DB() *gorm.DB {
	return r.db
}

// SaveSuccessfulInferences persists each successful prompt and merges them into
// a per-batch compiled JSON document in inference_compilations.
func (r *GormRepo) SaveSuccessfulInferences(payloads []plane.WorkItem) error {
	if len(payloads) == 0 {
		return nil
	}

	r.write.Lock()
	defer r.write.Unlock()

	return r.db.Transaction(func(tx *gorm.DB) error {
		now := time.Now().UTC()
		rows := make([]GormPrompt, len(payloads))
		byBatch := make(map[string][]InferenceItem, 4)

		for i, p := range payloads {
			rows[i] = GormPrompt{
				ID:           p.ID,
				BatchID:      p.BatchID,
				Prompt:       p.Prompt,
				ProcessedB64: p.Inference,
				CreatedAt:    now,
			}
			batchKey := p.BatchID
			if batchKey == "" {
				batchKey = p.ID
			}
			byBatch[batchKey] = append(byBatch[batchKey], InferenceItem{
				ID:        p.ID,
				Prompt:    p.Prompt,
				Inference: p.Inference,
				Attempts:  p.Attempts,
			})
		}

		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(rows, len(rows)).Error; err != nil {
			return err
		}

		for batchID, items := range byBatch {
			if err := appendCompilation(tx, batchID, items, now); err != nil {
				return err
			}
		}
		return nil
	})
}

func appendCompilation(tx *gorm.DB, batchID string, items []InferenceItem, now time.Time) error {
	var row GormInferenceCompilation
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("batch_id = ?", batchID).
		First(&row).Error

	doc := InferenceDocument{
		BatchID:    batchID,
		Inferences: nil,
		UpdatedAt:  now,
	}

	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		doc.Inferences = items
	case err != nil:
		return err
	default:
		if err := json.Unmarshal([]byte(row.ResultJSON), &doc); err != nil {
			return err
		}
		doc.Inferences = append(doc.Inferences, items...)
		doc.BatchID = batchID
		doc.UpdatedAt = now
	}

	doc.Count = len(doc.Inferences)
	raw, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	row = GormInferenceCompilation{
		BatchID:    batchID,
		ResultJSON: string(raw),
		Count:      doc.Count,
		UpdatedAt:  now,
	}
	return tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "batch_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"result_json", "count", "updated_at"}),
	}).Create(&row).Error
}

// GetCompiledInferences returns the compiled JSON document for a batch, if present.
func (r *GormRepo) GetCompiledInferences(batchID string) (InferenceDocument, error) {
	var row GormInferenceCompilation
	if err := r.db.Where("batch_id = ?", batchID).First(&row).Error; err != nil {
		return InferenceDocument{}, err
	}
	var doc InferenceDocument
	if err := json.Unmarshal([]byte(row.ResultJSON), &doc); err != nil {
		return InferenceDocument{}, err
	}
	return doc, nil
}

// SaveBatch is kept as an alias for SaveSuccessfulInferences.
func (r *GormRepo) SaveBatch(payloads []plane.WorkItem) error {
	return r.SaveSuccessfulInferences(payloads)
}
