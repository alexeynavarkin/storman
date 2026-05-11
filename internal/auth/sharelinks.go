package auth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/alexnav/storman/internal/rbac"
)

// ShareLinkService manages the share_links table. Web-only — see PLAN §3.3.
type ShareLinkService struct {
	pool  *pgxpool.Pool
	clock func() time.Time
}

func NewShareLinkService(pool *pgxpool.Pool) *ShareLinkService {
	return &ShareLinkService{pool: pool, clock: time.Now}
}

// SetClock overrides the time source. Intended for tests.
func (s *ShareLinkService) SetClock(fn func() time.Time) { s.clock = fn }

// CreateOpts configures a new share link.
type CreateOpts struct {
	NodeID    uuid.UUID
	Actions   rbac.Action
	TTL       time.Duration
	MaxUses   *int       // nil = unlimited (still bounded by TTL)
	CreatedBy *uuid.UUID // nil for system-generated links
}

// Create issues a new share link. The returned ShareLink.Token is the secret
// URL value — show it to the operator once and never persist it elsewhere.
//
// Admin scope is rejected per PLAN §3.3 — share links must not grant Admin.
func (s *ShareLinkService) Create(ctx context.Context, opts CreateOpts) (ShareLink, error) {
	if opts.Actions == 0 {
		return ShareLink{}, errors.New("auth: empty scope")
	}
	if opts.Actions&rbac.Admin != 0 {
		return ShareLink{}, errors.New("auth: share-link cannot grant Admin")
	}
	if opts.Actions&rbac.Traverse != 0 {
		return ShareLink{}, errors.New("auth: Traverse is computed, not grantable")
	}
	if opts.TTL <= 0 {
		return ShareLink{}, errors.New("auth: TTL must be positive")
	}
	token, err := newShareToken()
	if err != nil {
		return ShareLink{}, err
	}
	now := s.clock()
	link := ShareLink{
		Token:     token,
		NodeID:    opts.NodeID,
		Actions:   uint8(opts.Actions),
		ExpiresAt: now.Add(opts.TTL),
		MaxUses:   opts.MaxUses,
		UsedCount: 0,
		CreatedBy: opts.CreatedBy,
		CreatedAt: now,
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO share_links (token, node_id, actions, expires_at, max_uses, used_count, created_by, created_at)
		 VALUES ($1, $2, ($3::int4)::bit(8), $4, $5, 0, $6, $7)`,
		link.Token, link.NodeID, int32(link.Actions), link.ExpiresAt, link.MaxUses, link.CreatedBy, link.CreatedAt,
	)
	if err != nil {
		return ShareLink{}, fmt.Errorf("insert share_link: %w", err)
	}
	return link, nil
}

// Validate looks up a share link by token and checks it covers want. Errors:
//   - ErrNotFound : token does not exist
//   - ErrShareLinkExpired : past expires_at
//   - ErrShareLinkUsedUp : used_count >= max_uses
//   - ErrShareLinkScope : scope does not cover want
func (s *ShareLinkService) Validate(ctx context.Context, token string, want rbac.Action) (ShareLink, error) {
	link, err := s.fetch(ctx, token)
	if err != nil {
		return ShareLink{}, err
	}
	if !s.clock().Before(link.ExpiresAt) {
		return ShareLink{}, ErrShareLinkExpired
	}
	if link.MaxUses != nil && link.UsedCount >= *link.MaxUses {
		return ShareLink{}, ErrShareLinkUsedUp
	}
	if !rbac.Has(rbac.Action(link.Actions), want) {
		return ShareLink{}, ErrShareLinkScope
	}
	return link, nil
}

// Use atomically increments used_count, returning the updated link or one of
// the validation errors. Callers should use this rather than Validate when
// they are actually performing the action — Validate is for previews/UI.
func (s *ShareLinkService) Use(ctx context.Context, token string, want rbac.Action) (ShareLink, error) {
	link, err := s.Validate(ctx, token, want)
	if err != nil {
		return ShareLink{}, err
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE share_links SET used_count = used_count + 1
		 WHERE token = $1
		   AND ($2::int IS NULL OR used_count < $2::int)`,
		token, link.MaxUses,
	)
	if err != nil {
		return ShareLink{}, err
	}
	if tag.RowsAffected() != 1 {
		return ShareLink{}, ErrShareLinkUsedUp
	}
	link.UsedCount++
	return link, nil
}

// Revoke deletes a share link by token.
func (s *ShareLinkService) Revoke(ctx context.Context, token string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM share_links WHERE token = $1`, token)
	return err
}

// ListByCreator returns every share link created by userID, newest first.
// Used by the "my shares" UI; the token field is included so the operator can
// re-copy a URL they've lost track of.
func (s *ShareLinkService) ListByCreator(ctx context.Context, userID uuid.UUID) ([]ShareLink, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT token, node_id, (actions)::int, expires_at, max_uses, used_count, created_by, created_at
		 FROM share_links WHERE created_by = $1 ORDER BY created_at DESC`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]ShareLink, 0)
	for rows.Next() {
		var link ShareLink
		var actions int32
		if err := rows.Scan(&link.Token, &link.NodeID, &actions, &link.ExpiresAt,
			&link.MaxUses, &link.UsedCount, &link.CreatedBy, &link.CreatedAt); err != nil {
			return nil, err
		}
		link.Actions = uint8(actions)
		out = append(out, link)
	}
	return out, rows.Err()
}

// ListAll is the operator view — every active share link in the system.
// Admin-only by virtue of the calling HTTP handler.
func (s *ShareLinkService) ListAll(ctx context.Context) ([]ShareLink, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT token, node_id, (actions)::int, expires_at, max_uses, used_count, created_by, created_at
		 FROM share_links ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]ShareLink, 0)
	for rows.Next() {
		var link ShareLink
		var actions int32
		if err := rows.Scan(&link.Token, &link.NodeID, &actions, &link.ExpiresAt,
			&link.MaxUses, &link.UsedCount, &link.CreatedBy, &link.CreatedAt); err != nil {
			return nil, err
		}
		link.Actions = uint8(actions)
		out = append(out, link)
	}
	return out, rows.Err()
}

func (s *ShareLinkService) fetch(ctx context.Context, token string) (ShareLink, error) {
	var (
		link    ShareLink
		actions int32
	)
	err := s.pool.QueryRow(ctx,
		`SELECT token, node_id, (actions)::int, expires_at, max_uses, used_count, created_by, created_at
		 FROM share_links WHERE token = $1`, token,
	).Scan(&link.Token, &link.NodeID, &actions, &link.ExpiresAt,
		&link.MaxUses, &link.UsedCount, &link.CreatedBy, &link.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ShareLink{}, ErrNotFound
	}
	if err != nil {
		return ShareLink{}, err
	}
	link.Actions = uint8(actions)
	return link, nil
}

func newShareToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}
