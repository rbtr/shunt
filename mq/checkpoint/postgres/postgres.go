// Package postgres exposes shunt's Postgres-backed queue state store publicly
// so hosts can wire durable checkpoint + lease persistence without importing
// internal packages.
package postgres

import (
	"context"
	"database/sql"
	"time"

	internal "github.com/rbtr/shunt/internal/checkpoint/postgres"
	pub "github.com/rbtr/shunt/mq/checkpoint"
)

// Store is the Postgres-backed checkpoint store. It implements the public
// checkpoint.Store (LoadQueue/SaveQueue/DeleteQueue) and the engine's queue
// lease (AcquireLease) for replica coordination.
type Store = internal.Store

// New returns a Postgres-backed store for the given *sql.DB (pgx driver),
// namespaced by namespace (tenant/instance) when non-empty.
func New(db *sql.DB, namespace string) *Store { return internal.New(db, namespace) }

// ApplyMigrations creates the queue-state and queue-lease tables.
func ApplyMigrations(ctx context.Context, s *Store) error { return s.ApplyMigrations(ctx) }

// AcquireLease acquires the queue lease for replica coordination.
func AcquireLease(ctx context.Context, s *Store, key pub.QueueKey, holderID string, ttl time.Duration) (bool, error) {
	return s.AcquireLease(ctx, key, holderID, ttl)
}
