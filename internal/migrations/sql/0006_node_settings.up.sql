CREATE TABLE node_settings (
    node_id        uuid PRIMARY KEY REFERENCES nodes(id) ON DELETE CASCADE,
    max_versions   int,
    retention_days int,
    version_policy text,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);
