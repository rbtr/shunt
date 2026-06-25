// Package postgres provides a database/sql-backed Postgres queue checkpoint store.
package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/rbtr/shunt/internal/checkpoint"
)

// PostgresMigrationV1 creates the queue-state table used by Postgres.
//
//go:embed migrations/001_queue_state.sql
var PostgresMigrationV1 string

type rowScanner interface {
	Scan(dest ...any) error
}

type sqlExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) rowScanner
}

type activeBatchJSON struct {
	PRs            []pullRequestJSON `json:"prs"`
	StagingBranch  string            `json:"staging_branch"`
	StagingSHA     string            `json:"staging_sha"`
	BaseGeneration int               `json:"base_generation"`
	Outcome        string            `json:"outcome,omitempty"`
}

type pullRequestJSON struct {
	Number  int    `json:"number"`
	HeadSHA string `json:"head_sha"`
}

type stdDB struct {
	db *sql.DB
}

func (s stdDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.db.ExecContext(ctx, query, args...)
}

func (s stdDB) QueryRowContext(ctx context.Context, query string, args ...any) rowScanner {
	return s.db.QueryRowContext(ctx, query, args...)
}

// Store stores queue snapshots in a Postgres database. Call ApplyMigrations
// before the first LoadQueue or SaveQueue.
type Store struct {
	db sqlExecutor
}

// New returns a Postgres-backed Store using db. The caller owns opening
// and closing db, including registering the chosen Postgres driver.
func New(db *sql.DB) *Store {
	if db == nil {
		return &Store{}
	}
	return &Store{db: stdDB{db: db}}
}

// ApplyMigrations ensures the backing table and indexes exist.
func (p *Store) ApplyMigrations(ctx context.Context) error {
	if err := p.ready(); err != nil {
		return err
	}
	_, err := p.db.ExecContext(ctx, PostgresMigrationV1)
	if err != nil {
		return fmt.Errorf("state: apply postgres migrations: %w", err)
	}
	return nil
}

// SaveQueue upserts a complete queue snapshot.
func (p *Store) SaveQueue(ctx context.Context, snapshot checkpoint.QueueSnapshot) error {
	if err := p.ready(); err != nil {
		return err
	}
	if err := snapshot.Validate(); err != nil {
		return err
	}
	snapshot = snapshot.Clone()
	pending, err := json.Marshal(snapshot.Pending)
	if err != nil {
		return fmt.Errorf("state: marshal pending queue: %w", err)
	}
	active, err := json.Marshal(activeToJSON(snapshot.Active))
	if err != nil {
		return fmt.Errorf("state: marshal active batches: %w", err)
	}
	var linger any
	if !snapshot.LingerSince.IsZero() {
		linger = snapshot.LingerSince.UTC()
	}
	_, err = p.db.ExecContext(ctx, `
INSERT INTO shunt_queue_state (
    owner, repo, base, pending, active, linger_since, base_generation, staging_sequence, updated_at
) VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7, $8, now())
ON CONFLICT (owner, repo, base) DO UPDATE SET
    pending = EXCLUDED.pending,
    active = EXCLUDED.active,
    linger_since = EXCLUDED.linger_since,
    base_generation = EXCLUDED.base_generation,
    staging_sequence = EXCLUDED.staging_sequence,
    updated_at = now()
`, snapshot.Key.Owner, snapshot.Key.Repo, snapshot.Key.Base, string(pending), string(active), linger, snapshot.BaseGeneration, snapshot.StagingSequence)
	if err != nil {
		return fmt.Errorf("state: save queue %s/%s@%s: %w", snapshot.Key.Owner, snapshot.Key.Repo, snapshot.Key.Base, err)
	}
	return nil
}

// LoadQueue returns the stored snapshot for key. The boolean is false when the
// queue has no durable state yet.
func (p *Store) LoadQueue(ctx context.Context, key checkpoint.QueueKey) (checkpoint.QueueSnapshot, bool, error) {
	if err := p.ready(); err != nil {
		return checkpoint.QueueSnapshot{}, false, err
	}
	if err := key.Validate(); err != nil {
		return checkpoint.QueueSnapshot{}, false, err
	}
	var pendingRaw, activeRaw []byte
	var linger sql.NullTime
	var baseGeneration, stagingSequence int
	err := p.db.QueryRowContext(ctx, `
SELECT pending, active, linger_since, base_generation, staging_sequence
FROM shunt_queue_state
WHERE owner = $1 AND repo = $2 AND base = $3
`, key.Owner, key.Repo, key.Base).Scan(&pendingRaw, &activeRaw, &linger, &baseGeneration, &stagingSequence)
	if errors.Is(err, sql.ErrNoRows) {
		return checkpoint.QueueSnapshot{}, false, nil
	}
	if err != nil {
		return checkpoint.QueueSnapshot{}, false, fmt.Errorf("state: load queue %s/%s@%s: %w", key.Owner, key.Repo, key.Base, err)
	}
	var pending [][]int
	if err := json.Unmarshal(pendingRaw, &pending); err != nil {
		return checkpoint.QueueSnapshot{}, false, fmt.Errorf("state: decode pending queue: %w", err)
	}
	var activeJSON []activeBatchJSON
	if err := json.Unmarshal(activeRaw, &activeJSON); err != nil {
		return checkpoint.QueueSnapshot{}, false, fmt.Errorf("state: decode active batches: %w", err)
	}
	snapshot := checkpoint.QueueSnapshot{
		Key:             key,
		Pending:         pending,
		Active:          activeFromJSON(activeJSON),
		BaseGeneration:  baseGeneration,
		StagingSequence: stagingSequence,
	}
	if linger.Valid {
		snapshot.LingerSince = linger.Time
	}
	if err := snapshot.Validate(); err != nil {
		return checkpoint.QueueSnapshot{}, false, err
	}
	return snapshot.Clone(), true, nil
}

// DeleteQueue removes any durable state for key.
func (p *Store) DeleteQueue(ctx context.Context, key checkpoint.QueueKey) error {
	if err := p.ready(); err != nil {
		return err
	}
	if err := key.Validate(); err != nil {
		return err
	}
	_, err := p.db.ExecContext(ctx, `
DELETE FROM shunt_queue_state
WHERE owner = $1 AND repo = $2 AND base = $3
`, key.Owner, key.Repo, key.Base)
	if err != nil {
		return fmt.Errorf("state: delete queue %s/%s@%s: %w", key.Owner, key.Repo, key.Base, err)
	}
	return nil
}

func (p *Store) ready() error {
	if p == nil || p.db == nil {
		return errors.New("state: nil postgres database")
	}
	return nil
}

func activeToJSON(in []checkpoint.ActiveBatchSnapshot) []activeBatchJSON {
	if in == nil {
		return nil
	}
	out := make([]activeBatchJSON, len(in))
	for i, active := range in {
		prs := make([]pullRequestJSON, len(active.PRs))
		for j, pr := range active.PRs {
			prs[j] = pullRequestJSON{Number: pr.Number, HeadSHA: pr.HeadSHA}
		}
		out[i] = activeBatchJSON{
			PRs:            prs,
			StagingBranch:  active.StagingBranch,
			StagingSHA:     active.StagingSHA,
			BaseGeneration: active.BaseGeneration,
			Outcome:        active.Outcome,
		}
	}
	return out
}

func activeFromJSON(in []activeBatchJSON) []checkpoint.ActiveBatchSnapshot {
	if in == nil {
		return nil
	}
	out := make([]checkpoint.ActiveBatchSnapshot, len(in))
	for i, active := range in {
		prs := make([]checkpoint.PullRequestSnapshot, len(active.PRs))
		for j, pr := range active.PRs {
			prs[j] = checkpoint.PullRequestSnapshot{Number: pr.Number, HeadSHA: pr.HeadSHA}
		}
		out[i] = checkpoint.ActiveBatchSnapshot{
			PRs:            prs,
			StagingBranch:  active.StagingBranch,
			StagingSHA:     active.StagingSHA,
			BaseGeneration: active.BaseGeneration,
			Outcome:        active.Outcome,
		}
	}
	return out
}
