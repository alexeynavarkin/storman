package auth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SessionPolicy bounds session lifetime.
type SessionPolicy struct {
	// IdleTimeout extends the session forward from the last request.
	IdleTimeout time.Duration
	// AbsoluteTimeout caps total lifetime regardless of activity.
	AbsoluteTimeout time.Duration
}

// DefaultSessionPolicy: 24h idle, 30d absolute.
func DefaultSessionPolicy() SessionPolicy {
	return SessionPolicy{
		IdleTimeout:     24 * time.Hour,
		AbsoluteTimeout: 30 * 24 * time.Hour,
	}
}

// SessionService manages the sessions table.
type SessionService struct {
	pool   *pgxpool.Pool
	policy SessionPolicy
	clock  func() time.Time
}

func NewSessionService(pool *pgxpool.Pool) *SessionService {
	return &SessionService{
		pool:   pool,
		policy: DefaultSessionPolicy(),
		clock:  time.Now,
	}
}

// SetClock overrides the time source. Intended for tests.
func (s *SessionService) SetClock(fn func() time.Time) { s.clock = fn }

// SetPolicy overrides idle/absolute timeouts. Intended for tests and
// deployments that need non-default windows.
func (s *SessionService) SetPolicy(p SessionPolicy) { s.policy = p }

// Create issues a fresh session for the given user. The returned Session.ID
// is the secret cookie value; treat as sensitive.
func (s *SessionService) Create(ctx context.Context, userID uuid.UUID, ip *netip.Addr, userAgent string) (Session, error) {
	id, err := newSessionID()
	if err != nil {
		return Session{}, err
	}
	now := s.clock()
	expires := computeExpires(now, now, s.policy)
	sess := Session{
		ID:        id,
		UserID:    userID,
		CreatedAt: now,
		LastSeen:  now,
		ExpiresAt: expires,
		IP:        ip,
		UserAgent: userAgent,
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO sessions (id, user_id, created_at, last_seen, expires_at, ip, user_agent)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		sess.ID, sess.UserID, sess.CreatedAt, sess.LastSeen, sess.ExpiresAt, ip, nullString(userAgent),
	)
	if err != nil {
		return Session{}, fmt.Errorf("insert session: %w", err)
	}
	return sess, nil
}

// Validate looks up the session by ID and reports whether it is still alive.
// Returns ErrNotFound for unknown IDs and ErrSessionExpired for expired ones.
// The caller should normalize both into "please log in".
func (s *SessionService) Validate(ctx context.Context, id string) (Session, error) {
	sess, err := s.fetch(ctx, id)
	if err != nil {
		return Session{}, err
	}
	if !s.clock().Before(sess.ExpiresAt) {
		return Session{}, ErrSessionExpired
	}
	return sess, nil
}

// Touch updates last_seen and slides expires_at forward. Should be batched in
// real deployments — calling on every request is fine for low-traffic but
// expensive at scale.
func (s *SessionService) Touch(ctx context.Context, id string) error {
	now := s.clock()
	sess, err := s.fetch(ctx, id)
	if err != nil {
		return err
	}
	expires := computeExpires(sess.CreatedAt, now, s.policy)
	_, err = s.pool.Exec(ctx,
		`UPDATE sessions SET last_seen = $1, expires_at = $2 WHERE id = $3`,
		now, expires, id)
	return err
}

func (s *SessionService) Logout(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM sessions WHERE id = $1`, id)
	return err
}

func (s *SessionService) LogoutAll(ctx context.Context, userID uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM sessions WHERE user_id = $1`, userID)
	return err
}

// LogoutAllExcept terminates every session for userID except keepID. Useful
// after a self-initiated password change: other devices are kicked out, but
// the request that performed the change keeps working.
func (s *SessionService) LogoutAllExcept(ctx context.Context, userID uuid.UUID, keepID string) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM sessions WHERE user_id = $1 AND id <> $2`, userID, keepID)
	return err
}

func (s *SessionService) fetch(ctx context.Context, id string) (Session, error) {
	var sess Session
	var ua *string
	var ip *netip.Addr
	err := s.pool.QueryRow(ctx,
		`SELECT id, user_id, created_at, last_seen, expires_at, ip, user_agent
		 FROM sessions WHERE id = $1`, id,
	).Scan(&sess.ID, &sess.UserID, &sess.CreatedAt, &sess.LastSeen, &sess.ExpiresAt, &ip, &ua)
	if errors.Is(err, pgx.ErrNoRows) {
		return Session{}, ErrNotFound
	}
	if err != nil {
		return Session{}, err
	}
	sess.IP = ip
	if ua != nil {
		sess.UserAgent = *ua
	}
	return sess, nil
}

func newSessionID() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

// computeExpires bounds the new sliding-window expiry by the absolute timeout.
func computeExpires(createdAt, now time.Time, p SessionPolicy) time.Time {
	idle := now.Add(p.IdleTimeout)
	abs := createdAt.Add(p.AbsoluteTimeout)
	if idle.Before(abs) {
		return idle
	}
	return abs
}

func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
