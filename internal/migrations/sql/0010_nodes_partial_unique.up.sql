-- The table-level UNIQUE (parent_id, name) blocks reuse of a name after a
-- node has been soft-deleted. Replace it with a partial unique index that
-- applies only to active rows; soft-deleted nodes keep their original name
-- but no longer constrain new siblings.
ALTER TABLE nodes DROP CONSTRAINT nodes_parent_id_name_key;

CREATE UNIQUE INDEX nodes_parent_name_active_uidx
    ON nodes (parent_id, name)
    WHERE deleted_at IS NULL;
