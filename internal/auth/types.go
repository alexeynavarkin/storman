package auth

import (
	"errors"
	"net/netip"
	"time"

	"github.com/google/uuid"
)

// Sentinel errors. Service callers should compare against these via errors.Is.
var (
	ErrNotFound         = errors.New("auth: not found")
	ErrLoginTaken       = errors.New("auth: login already taken")
	ErrSessionExpired   = errors.New("auth: session expired")
	ErrShareLinkExpired = errors.New("auth: share link expired")
	ErrShareLinkUsedUp  = errors.New("auth: share link reached max uses")
	ErrShareLinkScope   = errors.New("auth: share link scope does not cover action")
	ErrAccountLocked    = errors.New("auth: account locked")
	ErrWeakPassword     = errors.New("auth: password too weak")
)

// User mirrors the persisted users row, minus the password hash, which is
// kept inside this package only.
type User struct {
	ID        uuid.UUID
	Login     string
	CreatedAt time.Time
	// LockedUntil is non-nil when the account is locked out due to brute-force
	// protection. Reset on successful authentication.
	LockedUntil *time.Time
}

// Session mirrors the persisted sessions row.
type Session struct {
	ID        string
	UserID    uuid.UUID
	CreatedAt time.Time
	LastSeen  time.Time
	ExpiresAt time.Time
	IP        *netip.Addr
	UserAgent string
}

// ShareLink mirrors the persisted share_links row.
type ShareLink struct {
	Token     string
	NodeID    uuid.UUID
	Actions   uint8
	ExpiresAt time.Time
	MaxUses   *int
	UsedCount int
	CreatedBy *uuid.UUID
	CreatedAt time.Time
}
