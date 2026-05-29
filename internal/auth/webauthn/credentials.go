package webauthn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/go-webauthn/webauthn/protocol"
	gowa "github.com/go-webauthn/webauthn/webauthn"
)

// protoTransport is an alias to the gowa transport enum so the file reads
// without import noise on every line that converts to/from text[].
type protoTransport = protocol.AuthenticatorTransport

// Credential is the row shape exposed to the web layer (list / rename / delete
// endpoints). It deliberately strips the raw credential ID and public key —
// those are package-internal.
type Credential struct {
	ID         uuid.UUID
	Name       string
	Transports []string
	BackupState bool
	CreatedAt  time.Time
	LastUsedAt *time.Time
}

// ListByUser returns the user's registered passkeys, newest first.
func (s *Service) ListByUser(ctx context.Context, userID uuid.UUID) ([]Credential, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, name, transports, backup_state, created_at, last_used_at
		 FROM webauthn_credentials WHERE user_id = $1 ORDER BY created_at DESC`,
		userID,
	)
	if err != nil {
		return nil, fmt.Errorf("list credentials: %w", err)
	}
	defer rows.Close()
	out := make([]Credential, 0)
	for rows.Next() {
		var c Credential
		if err := rows.Scan(&c.ID, &c.Name, &c.Transports, &c.BackupState, &c.CreatedAt, &c.LastUsedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// loadByUser returns the gowa.Credential slice needed to construct a
// wauthUser. The pool format includes the full credential record so go-webauthn
// can verify assertions.
func (s *Service) loadByUser(ctx context.Context, userID uuid.UUID) ([]gowa.Credential, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT credential_id, public_key, aaguid, sign_count, transports, backup_eligible, backup_state
		 FROM webauthn_credentials WHERE user_id = $1`,
		userID,
	)
	if err != nil {
		return nil, fmt.Errorf("load credentials: %w", err)
	}
	defer rows.Close()
	out := make([]gowa.Credential, 0)
	for rows.Next() {
		var (
			credID, pubKey, aaguid []byte
			signCount              int64
			transports             []string
			be, bs                 bool
		)
		if err := rows.Scan(&credID, &pubKey, &aaguid, &signCount, &transports, &be, &bs); err != nil {
			return nil, err
		}
		out = append(out, gowa.Credential{
			ID:        credID,
			PublicKey: pubKey,
			Authenticator: gowa.Authenticator{
				AAGUID:    aaguid,
				SignCount: uint32(signCount),
			},
			Flags: gowa.CredentialFlags{
				BackupEligible: be,
				BackupState:    bs,
			},
			Transport: transportsFromStrings(transports),
		})
	}
	return out, rows.Err()
}

// findByCredentialID looks a credential up by its raw WebAuthn ID — used by
// the discoverable-login handler to resolve which user owns an asserted
// credential. Returns ErrNotFound when no match.
func (s *Service) findByCredentialID(ctx context.Context, rawID []byte) (userID uuid.UUID, cred gowa.Credential, err error) {
	var (
		credID, pubKey, aaguid []byte
		signCount              int64
		transports             []string
		be, bs                 bool
	)
	err = s.pool.QueryRow(ctx,
		`SELECT user_id, credential_id, public_key, aaguid, sign_count, transports, backup_eligible, backup_state
		 FROM webauthn_credentials WHERE credential_id = $1`,
		rawID,
	).Scan(&userID, &credID, &pubKey, &aaguid, &signCount, &transports, &be, &bs)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, gowa.Credential{}, ErrNotFound
	}
	if err != nil {
		return uuid.Nil, gowa.Credential{}, err
	}
	cred = gowa.Credential{
		ID:        credID,
		PublicKey: pubKey,
		Authenticator: gowa.Authenticator{
			AAGUID:    aaguid,
			SignCount: uint32(signCount),
		},
		Flags: gowa.CredentialFlags{
			BackupEligible: be,
			BackupState:    bs,
		},
		Transport: transportsFromStrings(transports),
	}
	return userID, cred, nil
}

// insert persists a freshly registered credential. Returns ErrCredentialName
// when the (user, name) tuple collides with an existing row.
func (s *Service) insert(ctx context.Context, userID uuid.UUID, name string, cred *gowa.Credential) (Credential, error) {
	transports := transportsToStrings(cred.Transport)
	var (
		id        uuid.UUID
		createdAt time.Time
	)
	err := s.pool.QueryRow(ctx,
		`INSERT INTO webauthn_credentials
		   (user_id, credential_id, public_key, aaguid, sign_count, transports, backup_eligible, backup_state, name)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 RETURNING id, created_at`,
		userID, cred.ID, cred.PublicKey, cred.Authenticator.AAGUID,
		int64(cred.Authenticator.SignCount),
		transports, cred.Flags.BackupEligible, cred.Flags.BackupState, name,
	).Scan(&id, &createdAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			// Could be either name collision or credential_id collision —
			// name is the user-visible one, surface it specifically.
			if pgErr.ConstraintName == "webauthn_credentials_user_name_uq" {
				return Credential{}, ErrCredentialName
			}
		}
		return Credential{}, fmt.Errorf("insert credential: %w", err)
	}
	return Credential{
		ID:         id,
		Name:       name,
		Transports: transports,
		BackupState: cred.Flags.BackupState,
		CreatedAt:  createdAt,
	}, nil
}

// updateAfterLogin writes back the new sign count and last_used timestamp.
// Returns ErrSignCount when the asserted count is lower than what's stored —
// a strong cloned-authenticator signal. The caller should fail the login.
func (s *Service) updateAfterLogin(ctx context.Context, credentialID []byte, newCount uint32) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE webauthn_credentials
		   SET sign_count = $1, last_used_at = now()
		 WHERE credential_id = $2 AND sign_count <= $1`,
		int64(newCount), credentialID,
	)
	if err != nil {
		return fmt.Errorf("update credential: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// Row exists (we just read it) but the WHERE clause rejected the
		// update → the new count is below the stored one.
		return ErrSignCount
	}
	return nil
}

// Rename updates the user-facing label. Scoped by user_id to prevent one user
// renaming another's credential.
func (s *Service) Rename(ctx context.Context, userID, id uuid.UUID, name string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE webauthn_credentials SET name = $1 WHERE id = $2 AND user_id = $3`,
		name, id, userID,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrCredentialName
		}
		return fmt.Errorf("rename credential: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// Delete removes the credential. Scoped by user_id to prevent cross-user
// deletion.
func (s *Service) Delete(ctx context.Context, userID, id uuid.UUID) error {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM webauthn_credentials WHERE id = $1 AND user_id = $2`, id, userID,
	)
	if err != nil {
		return fmt.Errorf("delete credential: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// transportsToStrings converts the gowa transport enum slice to text[] for
// storage.
func transportsToStrings(ts []protoTransport) []string {
	if len(ts) == 0 {
		return []string{}
	}
	out := make([]string, 0, len(ts))
	for _, t := range ts {
		out = append(out, string(t))
	}
	return out
}

// transportsFromStrings inverts transportsToStrings.
func transportsFromStrings(ts []string) []protoTransport {
	if len(ts) == 0 {
		return nil
	}
	out := make([]protoTransport, 0, len(ts))
	for _, t := range ts {
		out = append(out, protoTransport(t))
	}
	return out
}

// challengeIDFromSession derives the storage key for a SessionData. The raw
// challenge bytes are unique per ceremony (32 random bytes from gowa) so we
// use them as the PK — no separate ID column needed. Stored as bytea, returned
// as []byte to the caller.
func challengeIDFromSession(sess *gowa.SessionData) ([]byte, error) {
	// Challenge is base64url-encoded in SessionData.
	return decodeB64URL(sess.Challenge)
}

// sessionDataJSON marshals a SessionData for storage. JSON is fine here: it
// preserves every field gowa needs, and we never look inside post-storage.
func sessionDataJSON(sess *gowa.SessionData) ([]byte, error) {
	return json.Marshal(sess)
}

func sessionDataFromJSON(data []byte) (*gowa.SessionData, error) {
	var sess gowa.SessionData
	if err := json.Unmarshal(data, &sess); err != nil {
		return nil, err
	}
	return &sess, nil
}
