CREATE TABLE audit_log (
    id        bigserial NOT NULL,
    ts        timestamptz NOT NULL DEFAULT now(),
    user_id   uuid,
    action    text NOT NULL,
    node_id   uuid,
    ip        inet,
    result    text NOT NULL,
    details   jsonb
) PARTITION BY RANGE (ts);

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
        p_name := 'audit_log_' || to_char(p_from, 'YYYYMM');
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF audit_log FOR VALUES FROM (%L) TO (%L)',
            p_name, p_from, p_to
        );
        EXECUTE format('CREATE INDEX %I ON %I (user_id, ts)', p_name || '_user_idx', p_name);
        EXECUTE format('CREATE INDEX %I ON %I (node_id, ts)', p_name || '_node_idx', p_name);
        EXECUTE format('CREATE INDEX %I ON %I (action, ts)', p_name || '_action_idx', p_name);
    END LOOP;
END $$;
