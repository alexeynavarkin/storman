package webauthn

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	gowa "github.com/go-webauthn/webauthn/webauthn"
)

// challengePurpose enumerates valid purpose tags. Must match the CHECK
// constraint in 0011_webauthn.up.sql.
type challengePurpose string

const (
	purposeRegister challengePurpose = "register"
	purposeLogin    challengePurpose = "login"
)

// saveChallenge persists a SessionData with TTL and returns the challenge
// bytes (which also serve as the row PK). The caller hands those bytes back
// to the browser as `challenge_id` and supplies them to consumeChallenge on
// finish.
func (s *Service) saveChallenge(ctx context.Context, purpose challengePurpose, userID *uuid.UUID, sess *gowa.SessionData) ([]byte, error) {
	id, err := challengeIDFromSession(sess)
	if err != nil {
		return nil, fmt.Errorf("decode challenge: %w", err)
	}
	data, err := sessionDataJSON(sess)
	if err != nil {
		return nil, fmt.Errorf("marshal session: %w", err)
	}
	expires := s.clock().Add(s.ttl)
	_, err = s.pool.Exec(ctx,
		`INSERT INTO webauthn_challenges (id, purpose, user_id, session_data, expires_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		id, string(purpose), userID, data, expires,
	)
	if err != nil {
		return nil, fmt.Errorf("insert challenge: %w", err)
	}
	return id, nil
}

// consumeChallenge atomically deletes the row and returns its payload.
// Concurrent finishers race-lose: only one DELETE returns a row. Rows whose
// expires_at is in the past are treated as missing.
func (s *Service) consumeChallenge(ctx context.Context, id []byte, purpose challengePurpose) (*gowa.SessionData, *uuid.UUID, error) {
	var (
		data    []byte
		userID  *uuid.UUID
	)
	err := s.pool.QueryRow(ctx,
		`DELETE FROM webauthn_challenges
		   WHERE id = $1 AND purpose = $2 AND expires_at > now()
		 RETURNING session_data, user_id`,
		id, string(purpose),
	).Scan(&data, &userID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, ErrChallengeExpired
	}
	if err != nil {
		return nil, nil, fmt.Errorf("consume challenge: %w", err)
	}
	sess, err := sessionDataFromJSON(data)
	if err != nil {
		return nil, nil, fmt.Errorf("decode session: %w", err)
	}
	return sess, userID, nil
}

// SweepExpired removes rows whose TTL has passed. Returns the count.
// Called by the background sweeper in the web package.
func (s *Service) SweepExpired(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM webauthn_challenges WHERE expires_at <= now()`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
