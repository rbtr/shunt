CREATE TABLE IF NOT EXISTS shunt_queue_leases (
    owner text NOT NULL,
    repo text NOT NULL,
    base text NOT NULL,
    holder_id text NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (owner, repo, base)
);
