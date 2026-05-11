DROP INDEX IF EXISTS nodes_parent_name_active_uidx;
ALTER TABLE nodes ADD CONSTRAINT nodes_parent_id_name_key UNIQUE (parent_id, name);
