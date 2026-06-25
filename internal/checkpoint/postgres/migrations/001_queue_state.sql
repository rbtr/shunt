CREATE TABLE IF NOT EXISTS shunt_queue_state (
    owner text NOT NULL,
    repo text NOT NULL,
    base text NOT NULL,
    pending jsonb NOT NULL DEFAULT '[]'::jsonb,
    active jsonb NOT NULL DEFAULT '[]'::jsonb,
    linger_since timestamptz,
    base_generation integer NOT NULL DEFAULT 0,
    staging_sequence integer NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (owner, repo, base)
);

CREATE INDEX IF NOT EXISTS shunt_queue_state_updated_at_idx
    ON shunt_queue_state (updated_at);
