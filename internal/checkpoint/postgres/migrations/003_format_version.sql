-- Add the checkpoint format version to the Postgres queue-state store.
--
-- The store persists the snapshot as individual columns, not a JSON blob, so
-- QueueSnapshot.FormatVersion was silently dropped on save and always read
-- back as 0 — unlike the bolt store, which json.Marshals the whole struct.
-- Existing rows default to 0; the engine treats a below-current version with
-- in-flight work as a legacy checkpoint it cannot resume exactly, discards it,
-- and re-derives the queue from the forge.
ALTER TABLE shunt_queue_state
    ADD COLUMN IF NOT EXISTS format_version integer NOT NULL DEFAULT 0;
