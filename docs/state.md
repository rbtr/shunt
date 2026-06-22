# Durable queue state

shunt's default durable store is the built-in bbolt implementation enabled by
`SHUNT_STATE_PATH`. The `internal/checkpoint/postgres` package is a reference
Postgres implementation of the same engine checkpoint contract for deployments
that need an external database-backed store.

The Postgres store persists one snapshot per `(owner, repo, base)` queue in
`shunt_queue_state`:

- pending candidate batches (`pending` JSONB)
- active staging batches, including PR numbers, tested head SHAs, staging branch/SHA, base generation, and terminal outcome (`active` JSONB)
- batch-linger start time
- base generation and staging branch sequence counters

Call `postgres.New(db).ApplyMigrations(ctx)` once at startup before using the
store. Runtime configuration is intentionally not exposed yet; bbolt remains the
built-in configured store while external stores settle behind the shared
checkpoint boundary.
