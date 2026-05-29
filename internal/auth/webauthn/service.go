// Package webauthn implements passkey (WebAuthn) authentication for the web
// UI. Storman keeps passwords as a fallback method; passkeys add a stronger,
// phishing-resistant primary path. This package owns the credential and
// challenge tables, the integration with go-webauthn, and the flow
// orchestration (Begin/Finish for register/login). It deliberately knows
// nothing about HTTP or cookies — the web package adapts handlers and session
// issuance on top of FinishLogin.
//
// Scope:
//   - Only the web UI session login uses passkeys; WebDAV/FTP/share-links
//     continue to use app-passwords. Discoverable (usernameless) credentials
//     are used so the login flow does not require a login string.
//   - Registration always happens inside an authenticated session (the web
//     handler enforces this via authedMutate). The package does not bootstrap
//     unauthenticated users from a passkey.
package webauthn

import (
	"errors"
	"fmt"
	"time"

	gowa "github.com/go-webauthn/webauthn/webauthn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultChallengeTTL bounds how long an unfinished register/login ceremony
// stays valid. 5 minutes covers normal browser/authenticator latency without
// leaving stale rows around.
const DefaultChallengeTTL = 5 * time.Minute

// Config configures the package at construction time. RPID + RPOrigins map
// directly to gowa.Config; ChallengeTTL falls back to DefaultChallengeTTL
// when zero.
type Config struct {
	RPID          string
	RPDisplayName string
	RPOrigins     []string
	ChallengeTTL  time.Duration
}

// Sentinel errors used by callers (the HTTP layer maps these to status codes).
var (
	ErrChallengeExpired = errors.New("webauthn: challenge expired or unknown")
	ErrCredentialName   = errors.New("webauthn: credential name already exists")
	ErrSignCount        = errors.New("webauthn: sign count regression (possible cloned authenticator)")
	ErrNotFound         = errors.New("webauthn: credential not found")
)

// Service owns all passkey-related state. It is safe for concurrent use.
type Service struct {
	pool  *pgxpool.Pool
	rp    *gowa.WebAuthn
	ttl   time.Duration
	clock func() time.Time
}

// NewService constructs the service. RPID must be non-empty (callers gate on
// config.Enabled() before reaching this point). At least one origin is
// required; go-webauthn rejects an empty list.
func NewService(pool *pgxpool.Pool, cfg Config) (*Service, error) {
	if cfg.RPID == "" {
		return nil, errors.New("webauthn: RPID is required")
	}
	if len(cfg.RPOrigins) == 0 {
		return nil, errors.New("webauthn: at least one RPOrigin is required")
	}
	displayName := cfg.RPDisplayName
	if displayName == "" {
		displayName = "Storman"
	}
	rp, err := gowa.New(&gowa.Config{
		RPID:          cfg.RPID,
		RPDisplayName: displayName,
		RPOrigins:     cfg.RPOrigins,
	})
	if err != nil {
		return nil, fmt.Errorf("webauthn: build relying party: %w", err)
	}
	ttl := cfg.ChallengeTTL
	if ttl <= 0 {
		ttl = DefaultChallengeTTL
	}
	return &Service{
		pool:  pool,
		rp:    rp,
		ttl:   ttl,
		clock: time.Now,
	}, nil
}

// SetClock overrides the time source. Test-only.
func (s *Service) SetClock(fn func() time.Time) { s.clock = fn }

// RP returns the underlying go-webauthn relying party. Exposed for tests that
// need to drive the library directly (e.g. with the test authenticator).
func (s *Service) RP() *gowa.WebAuthn { return s.rp }

// Pool exposes the pgx pool for tests that share state.
func (s *Service) Pool() *pgxpool.Pool { return s.pool }
