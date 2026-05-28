CREATE TABLE webauthn_credentials (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    credential_id   bytea NOT NULL,
    public_key      bytea NOT NULL,
    aaguid          bytea NOT NULL DEFAULT '\x'::bytea,
    sign_count      bigint NOT NULL DEFAULT 0 CHECK (sign_count >= 0),
    transports      text[] NOT NULL DEFAULT '{}',
    backup_eligible boolean NOT NULL DEFAULT false,
    backup_state    boolean NOT NULL DEFAULT false,
    name            citext NOT NULL CHECK (length(name) BETWEEN 1 AND 64),
    created_at      timestamptz NOT NULL DEFAULT now(),
    last_used_at    timestamptz
);
CREATE UNIQUE INDEX webauthn_credentials_credential_id_uq ON webauthn_credentials (credential_id);
CREATE INDEX        webauthn_credentials_user_id_idx     ON webauthn_credentials (user_id);
CREATE UNIQUE INDEX webauthn_credentials_user_name_uq    ON webauthn_credentials (user_id, name);

CREATE TABLE webauthn_challenges (
    id           bytea PRIMARY KEY,
    purpose      text  NOT NULL CHECK (purpose IN ('register','login')),
    user_id      uuid  REFERENCES users(id) ON DELETE CASCADE,
    session_data jsonb NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz NOT NULL
);
CREATE INDEX webauthn_challenges_expires_idx ON webauthn_challenges (expires_at);
