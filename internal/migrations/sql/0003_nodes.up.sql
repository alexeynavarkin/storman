CREATE TYPE node_type   AS ENUM ('file', 'dir');
CREATE TYPE node_status AS ENUM ('pending', 'ready', 'deleted', 'broken');

CREATE TABLE nodes (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    parent_id     uuid REFERENCES nodes(id),
    path          ltree NOT NULL,
    name          text NOT NULL,
    type          node_type NOT NULL,
    backend_kind  text,
    backend_ref   text,
    size          bigint,
    mime          text,
    mtime         timestamptz,
    sha256        bytea,
    status        node_status NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    deleted_at    timestamptz,
    UNIQUE (parent_id, name),
    CHECK (type <> 'file' OR (backend_kind IS NOT NULL AND backend_ref IS NOT NULL)),
    CHECK (type <> 'dir'  OR backend_ref IS NULL),
    CHECK (sha256 IS NULL OR octet_length(sha256) = 32)
);

CREATE INDEX nodes_path_gist     ON nodes USING gist (path);
CREATE INDEX nodes_parent_id_idx ON nodes(parent_id);
CREATE INDEX nodes_sha256_idx    ON nodes(sha256) WHERE sha256 IS NOT NULL;
CREATE INDEX nodes_deleted_idx   ON nodes(deleted_at) WHERE deleted_at IS NOT NULL;
