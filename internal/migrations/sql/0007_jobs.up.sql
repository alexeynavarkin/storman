CREATE TABLE jobs (
    id           bigserial PRIMARY KEY,
    node_id      uuid NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    kind         text NOT NULL,
    status       text NOT NULL,
    attempts     int NOT NULL DEFAULT 0,
    last_error   text,
    locked_until timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX jobs_pick_idx    ON jobs(kind, status, created_at);
CREATE INDEX jobs_node_id_idx ON jobs(node_id);

CREATE TABLE jobs_history (
    id           bigint NOT NULL,
    node_id      uuid,
    kind         text NOT NULL,
    final_status text NOT NULL,
    attempts     int NOT NULL,
    last_error   text,
    enqueued_at  timestamptz NOT NULL,
    finished_at  timestamptz NOT NULL DEFAULT now()
) PARTITION BY RANGE (finished_at);

DO $$
DECLARE
    start_month date := date_trunc('month', now())::date;
    i int;
    p_from date;
    p_to date;
    p_name text;
BEGIN
    FOR i IN 0..4 LOOP
        p_from := (start_month + (i || ' months')::interval)::date;
        p_to   := (start_month + ((i+1) || ' months')::interval)::date;
        p_name := 'jobs_history_' || to_char(p_from, 'YYYYMM');
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF jobs_history FOR VALUES FROM (%L) TO (%L)',
            p_name, p_from, p_to
        );
        EXECUTE format('CREATE INDEX %I ON %I (node_id, finished_at)', p_name || '_node_idx', p_name);
        EXECUTE format('CREATE INDEX %I ON %I (kind, final_status)', p_name || '_kind_idx', p_name);
    END LOOP;
END $$;
