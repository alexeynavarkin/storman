CREATE TABLE permissions (
    id         bigserial PRIMARY KEY,
    node_id    uuid NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    user_id    uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    actions    bit(8) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (node_id, user_id)
);
CREATE INDEX permissions_user_id_idx ON permissions(user_id);

CREATE TABLE share_links (
    token       text PRIMARY KEY,
    node_id     uuid NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    actions     bit(8) NOT NULL,
    expires_at  timestamptz NOT NULL,
    max_uses    int,
    used_count  int NOT NULL DEFAULT 0,
    created_by  uuid REFERENCES users(id),
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX share_links_node_id_idx    ON share_links(node_id);
CREATE INDEX share_links_expires_at_idx ON share_links(expires_at);
