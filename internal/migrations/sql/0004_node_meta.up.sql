CREATE TABLE node_meta (
    node_id     uuid PRIMARY KEY REFERENCES nodes(id) ON DELETE CASCADE,
    width       int,
    height      int,
    taken_at    timestamptz,
    lat         double precision,
    lng         double precision,
    duration_ms int,
    extra       jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX node_meta_extra_gin    ON node_meta USING gin (extra jsonb_path_ops);
CREATE INDEX node_meta_taken_at_idx ON node_meta(taken_at);
CREATE INDEX node_meta_geo_idx      ON node_meta(lat, lng) WHERE lat IS NOT NULL AND lng IS NOT NULL;
