-- Store the whole QueueSnapshot as one JSON document.
--
-- The store was written as a fixed set of columns (pending, active,
-- linger_since, base_generation, staging_sequence, format_version). Every
-- field added to QueueSnapshot since — the bisection Trees, PendingNodes,
-- the TransitionOutbox, and the per-batch RunID / BaseAnchor / ExactKey /
-- LineagePath — was silently dropped on save and read back as zero, because
-- nothing here knew about it. The bolt and in-memory stores json.Marshal the
-- whole struct and were unaffected; production runs on Postgres.
--
-- From now on SaveQueue writes the full snapshot into `snapshot` and LoadQueue
-- reads from it. The individual columns are still written (external queries and
-- the updated_at index depend on them) but are no longer the source of truth.
-- Rows written before this migration have snapshot IS NULL; LoadQueue falls
-- back to reconstructing what it can from the columns for those.
ALTER TABLE shunt_queue_state
    ADD COLUMN IF NOT EXISTS snapshot jsonb;
