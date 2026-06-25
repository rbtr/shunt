# Durable queue state

shunt can persist queue checkpoints with either the local bbolt implementation
enabled by `SHUNT_STATE_PATH` or the Postgres implementation enabled by
`SHUNT_POSTGRES_DSN`. Leave both unset to keep in-memory queue state, and do not
set both at once.

The Postgres store persists one snapshot per `(owner, repo, base)` queue in
`shunt_queue_state`:

- pending candidate batches (`pending` JSONB)
- active staging batches, including PR numbers, tested head SHAs, staging branch/SHA, base generation, and terminal outcome (`active` JSONB)
- batch-linger start time
- base generation and staging branch sequence counters

When `SHUNT_POSTGRES_DSN` is set, shunt opens the DSN with pgx's `database/sql`
driver and applies the embedded migration at startup before using the store. Keep
the DSN in a runtime secret store rather than in repository files.
