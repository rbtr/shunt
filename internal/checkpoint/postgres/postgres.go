// Package postgres provides a database/sql-backed Postgres queue checkpoint store.
package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rbtr/shunt/internal/checkpoint"
)

// PostgresMigrationV1 creates the queue-state table used by Postgres.
//
//go:embed migrations/001_queue_state.sql
var PostgresMigrationV1 string

// PostgresMigrationV2 creates the queue-lease table used to coordinate replicas.
//
//go:embed migrations/002_queue_leases.sql
var PostgresMigrationV2 string

// PostgresMigrationV3 adds the checkpoint format version column.
//
//go:embed migrations/003_format_version.sql
var PostgresMigrationV3 string

// PostgresMigrationV4 adds the full-snapshot JSON column so no QueueSnapshot
// field can be silently dropped by a store that predates it.
//
//go:embed migrations/004_snapshot_blob.sql
var PostgresMigrationV4 string

type rowScanner interface {
	Scan(dest ...any) error
}

type sqlExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) rowScanner
}

type activeBatchJSON struct {
	PRs                []pullRequestJSON `json:"prs"`
	StagingBranch      string            `json:"staging_branch"`
	StagingSHA         string            `json:"staging_sha"`
	BaseGeneration     int               `json:"base_generation"`
	Outcome            string            `json:"outcome,omitempty"`
	PhaseSince         time.Time         `json:"phase_since,omitempty"`
	MissingGateRetries int               `json:"missing_gate_retries,omitempty"`
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
	db       sqlExecutor
	stateTbl string
	leaseTbl string
}

// New returns a Postgres-backed Store using db. namespace (optional) suffixes
// the table names (shunt_queue_state_<ns>) so distinct tenants/forge
// instances never share queue-state rows. The caller owns opening and closing
// db, including registering the chosen Postgres driver.
func New(db *sql.DB, namespace string) *Store {
	if db == nil {
		return &Store{}
	}
	ns := sanitizeNamespace(namespace)
	stateTbl, leaseTbl := "shunt_queue_state", "shunt_queue_leases"
	if ns != "" {
		stateTbl += "_" + ns
		leaseTbl += "_" + ns
	}
	return &Store{db: stdDB{db: db}, stateTbl: stateTbl, leaseTbl: leaseTbl}
}

// sanitizeNamespace restricts a namespace to [a-z0-9_] so it can safely
// suffix table names.
func sanitizeNamespace(ns string) string {
	var b strings.Builder
	for _, r := range ns {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// ApplyMigrations ensures the backing tables and indexes exist.
func (p *Store) ApplyMigrations(ctx context.Context) error {
	if err := p.ready(); err != nil {
		return err
	}
	for _, migration := range []string{PostgresMigrationV1, PostgresMigrationV2, PostgresMigrationV3, PostgresMigrationV4} {
		m := strings.ReplaceAll(migration, "shunt_queue_state", p.stateTbl)
		m = strings.ReplaceAll(m, "shunt_queue_leases", p.leaseTbl)
		if _, err := p.db.ExecContext(ctx, m); err != nil {
			return fmt.Errorf("state: apply postgres migrations: %w", err)
		}
	}
	return nil
}

// AcquireLease atomically acquires or renews a lease for key. It returns false,
// nil when another holder has an unexpired lease. A holder can renew its own
// lease, and a different holder can take over after expiry.
func (p *Store) AcquireLease(ctx context.Context, key checkpoint.QueueKey, holderID string, ttl time.Duration) (bool, error) {
	if err := p.ready(); err != nil {
		return false, err
	}
	if err := key.Validate(); err != nil {
		return false, err
	}
	if holderID == "" {
		return false, errors.New("state: queue lease holder ID is required")
	}
	if ttl < time.Microsecond {
		return false, errors.New("state: queue lease TTL must be at least one microsecond")
	}

	var expiresAt time.Time
	err := p.db.QueryRowContext(ctx, fmt.Sprintf(`
INSERT INTO %s (owner, repo, base, holder_id, expires_at)
VALUES ($1, $2, $3, $4, now() + $5 * interval '1 microsecond')
ON CONFLICT (owner, repo, base) DO UPDATE SET
    holder_id = EXCLUDED.holder_id,
    expires_at = EXCLUDED.expires_at
WHERE %s.holder_id = EXCLUDED.holder_id
   OR %s.expires_at <= now()
RETURNING expires_at
`, p.leaseTbl, p.leaseTbl, p.leaseTbl), key.Owner, key.Repo, key.Base, holderID, ttl.Microseconds()).Scan(&expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("state: acquire queue lease %s/%s@%s: %w", key.Owner, key.Repo, key.Base, err)
	}
	return true, nil
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
	// The whole snapshot, verbatim. This is the source of truth on load; the
	// individual columns above are kept for external queries and the
	// updated_at index but never lose a field the way hand-picked columns do.
	full, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("state: marshal snapshot: %w", err)
	}
	var linger any
	if !snapshot.LingerSince.IsZero() {
		linger = snapshot.LingerSince.UTC()
	}
	_, err = p.db.ExecContext(ctx, fmt.Sprintf(`
INSERT INTO %s (
    owner, repo, base, pending, active, linger_since, base_generation, staging_sequence, format_version, snapshot, updated_at
) VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7, $8, $9, $10::jsonb, now())
ON CONFLICT (owner, repo, base) DO UPDATE SET
    pending = EXCLUDED.pending,
    active = EXCLUDED.active,
    linger_since = EXCLUDED.linger_since,
    base_generation = EXCLUDED.base_generation,
    staging_sequence = EXCLUDED.staging_sequence,
    format_version = EXCLUDED.format_version,
    snapshot = EXCLUDED.snapshot,
    updated_at = now()
`, p.stateTbl), snapshot.Key.Owner, snapshot.Key.Repo, snapshot.Key.Base, string(pending), string(active), linger, snapshot.BaseGeneration, snapshot.StagingSequence, snapshot.FormatVersion, string(full))
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
	var pendingRaw, activeRaw, snapshotRaw []byte
	var linger sql.NullTime
	var baseGeneration, stagingSequence, formatVersion int
	err := p.db.QueryRowContext(ctx, fmt.Sprintf(`
SELECT pending, active, linger_since, base_generation, staging_sequence, format_version, snapshot
FROM %s
WHERE owner = $1 AND repo = $2 AND base = $3
`, p.stateTbl), key.Owner, key.Repo, key.Base).Scan(&pendingRaw, &activeRaw, &linger, &baseGeneration, &stagingSequence, &formatVersion, &snapshotRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return checkpoint.QueueSnapshot{}, false, nil
	}
	if err != nil {
		return checkpoint.QueueSnapshot{}, false, fmt.Errorf("state: load queue %s/%s@%s: %w", key.Owner, key.Repo, key.Base, err)
	}
	// Preferred path: the row carries the whole snapshot. Rows written before
	// migration 004 have snapshot IS NULL and fall through to the columns.
	if len(snapshotRaw) > 0 {
		var snapshot checkpoint.QueueSnapshot
		if err := json.Unmarshal(snapshotRaw, &snapshot); err != nil {
			return checkpoint.QueueSnapshot{}, false, fmt.Errorf("state: decode snapshot: %w", err)
		}
		snapshot.Key = key
		if err := snapshot.Validate(); err != nil {
			return checkpoint.QueueSnapshot{}, false, err
		}
		return snapshot.Clone(), true, nil
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
		FormatVersion:   formatVersion,
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
	_, err := p.db.ExecContext(ctx, fmt.Sprintf(`
DELETE FROM %s
WHERE owner = $1 AND repo = $2 AND base = $3
`, p.stateTbl), key.Owner, key.Repo, key.Base)
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
			PRs:                prs,
			StagingBranch:      active.StagingBranch,
			StagingSHA:         active.StagingSHA,
			BaseGeneration:     active.BaseGeneration,
			Outcome:            active.Outcome,
			PhaseSince:         active.PhaseSince,
			MissingGateRetries: active.MissingGateRetries,
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
			PRs:                prs,
			StagingBranch:      active.StagingBranch,
			StagingSHA:         active.StagingSHA,
			BaseGeneration:     active.BaseGeneration,
			Outcome:            active.Outcome,
			PhaseSince:         active.PhaseSince,
			MissingGateRetries: active.MissingGateRetries,
		}
	}
	return out
}
