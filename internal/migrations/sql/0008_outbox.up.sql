CREATE TABLE outbox (
    id           bigserial PRIMARY KEY,
    op           text NOT NULL,
    node_id      uuid,
    payload      jsonb NOT NULL,
    status       text NOT NULL,
    attempts     int NOT NULL DEFAULT 0,
    created_at   timestamptz NOT NULL DEFAULT now(),
    locked_until timestamptz
);
CREATE INDEX outbox_pick_idx
    ON outbox(status, locked_until)
    WHERE status IN ('pending', 'in_progress');

CREATE TABLE outbox_history (
    id            bigint NOT NULL,
    op            text NOT NULL,
    node_id       uuid,
    payload       jsonb NOT NULL,
    final_status  text NOT NULL,
    attempts      int NOT NULL,
    last_error    text,
    enqueued_at   timestamptz NOT NULL,
    finished_at   timestamptz NOT NULL DEFAULT now()
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
        p_name := 'outbox_history_' || to_char(p_from, 'YYYYMM');
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF outbox_history FOR VALUES FROM (%L) TO (%L)',
            p_name, p_from, p_to
        );
        EXECUTE format('CREATE INDEX %I ON %I (node_id, finished_at)', p_name || '_node_idx', p_name);
        EXECUTE format('CREATE INDEX %I ON %I (op, final_status, finished_at)', p_name || '_op_idx', p_name);
    END LOOP;
END $$;
