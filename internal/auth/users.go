package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// UserService manages the users table.
type UserService struct {
	pool   *pgxpool.Pool
	params HashParams
	clock  func() time.Time
	// MinPasswordLen enforces a minimal password length. 0 disables the check.
	MinPasswordLen int
}

func NewUserService(pool *pgxpool.Pool) *UserService {
	return &UserService{
		pool:           pool,
		params:         DefaultHashParams(),
		clock:          time.Now,
		MinPasswordLen: 12,
	}
}

// Pool exposes the underlying pool — useful for tests sharing one pool
// across services.
func (s *UserService) Pool() *pgxpool.Pool { return s.pool }

// SetTestParams overrides the argon2id cost. Intended for tests only —
// production code should rely on DefaultHashParams.
func (s *UserService) SetTestParams(p HashParams) { s.params = p }

// SetClock overrides the time source. Intended for tests.
func (s *UserService) SetClock(fn func() time.Time) { s.clock = fn }

// Create stores a new user. Login is trimmed but matched case-insensitively
// by the citext column. Returns ErrLoginTaken if the login already exists.
func (s *UserService) Create(ctx context.Context, login, password string) (User, error) {
	login = strings.TrimSpace(login)
	if login == "" {
		return User{}, errors.New("auth: empty login")
	}
	if s.MinPasswordLen > 0 && len(password) < s.MinPasswordLen {
		return User{}, fmt.Errorf("%w: at least %d characters required", ErrWeakPassword, s.MinPasswordLen)
	}
	hash, err := HashPassword(password, s.params)
	if err != nil {
		return User{}, err
	}
	var u User
	err = s.pool.QueryRow(ctx,
		`INSERT INTO users (login, password_hash) VALUES ($1, $2)
		 RETURNING id, login, created_at, locked_until`,
		login, hash,
	).Scan(&u.ID, &u.Login, &u.CreatedAt, &u.LockedUntil)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return User{}, ErrLoginTaken
		}
		return User{}, fmt.Errorf("insert user: %w", err)
	}
	return u, nil
}

// FindByLogin returns the user with the given login, or ErrNotFound.
func (s *UserService) FindByLogin(ctx context.Context, login string) (User, error) {
	var u User
	err := s.pool.QueryRow(ctx,
		`SELECT id, login, created_at, locked_until FROM users WHERE login = $1`,
		login,
	).Scan(&u.ID, &u.Login, &u.CreatedAt, &u.LockedUntil)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrNotFound
	}
	return u, err
}

// List returns every user, ordered by login. Used by the UI to populate
// permission-grant pickers. Result excludes password hashes by construction.
func (s *UserService) List(ctx context.Context) ([]User, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, login, created_at, locked_until FROM users ORDER BY login`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]User, 0)
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.Login, &u.CreatedAt, &u.LockedUntil); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// FindByID returns the user with the given ID, or ErrNotFound.
func (s *UserService) FindByID(ctx context.Context, id uuid.UUID) (User, error) {
	var u User
	err := s.pool.QueryRow(ctx,
		`SELECT id, login, created_at, locked_until FROM users WHERE id = $1`, id,
	).Scan(&u.ID, &u.Login, &u.CreatedAt, &u.LockedUntil)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrNotFound
	}
	return u, err
}

// Delete removes the user. FK cascades clean up sessions, app_passwords, and
// permissions. ErrNotFound if the user does not exist.
func (s *UserService) Delete(ctx context.Context, id uuid.UUID) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM users WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete user: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdatePassword replaces the user's password hash and resets the brute-force
// counters. Callers that want to invalidate active sessions should do so via
// SessionService.LogoutAll / LogoutAllExcept after this returns.
func (s *UserService) UpdatePassword(ctx context.Context, id uuid.UUID, password string) error {
	if s.MinPasswordLen > 0 && len(password) < s.MinPasswordLen {
		return fmt.Errorf("%w: at least %d characters required", ErrWeakPassword, s.MinPasswordLen)
	}
	hash, err := HashPassword(password, s.params)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE users
		 SET password_hash = $1, failed_attempts = 0, locked_until = NULL
		 WHERE id = $2`,
		hash, id)
	if err != nil {
		return fmt.Errorf("update password: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// Authenticate verifies login+password. On success it returns the user and
// resets failed_attempts. On password mismatch it bumps failed_attempts and,
// past the threshold, sets locked_until. Errors:
//   - ErrNotFound      : login does not exist (do not leak this to the client;
//     callers should map it to ErrPasswordMismatch externally).
//   - ErrAccountLocked : account is currently in cooldown.
//   - ErrPasswordMismatch : password does not match.
func (s *UserService) Authenticate(ctx context.Context, login, password string) (User, error) {
	var (
		id           uuid.UUID
		dbLogin      string
		hash         string
		createdAt    time.Time
		failed       int
		lockedUntil  *time.Time
		nowFn        = s.clock
	)
	err := s.pool.QueryRow(ctx,
		`SELECT id, login, password_hash, created_at, failed_attempts, locked_until
		 FROM users WHERE login = $1`,
		login,
	).Scan(&id, &dbLogin, &hash, &createdAt, &failed, &lockedUntil)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrNotFound
	}
	if err != nil {
		return User{}, err
	}
	if lockedUntil != nil && nowFn().Before(*lockedUntil) {
		return User{}, ErrAccountLocked
	}
	if err := VerifyPassword(password, hash); err != nil {
		if errors.Is(err, ErrPasswordMismatch) {
			s.recordFailure(ctx, id, failed+1)
		}
		return User{}, err
	}
	// Success → reset counters.
	if _, err := s.pool.Exec(ctx,
		`UPDATE users SET failed_attempts = 0, locked_until = NULL WHERE id = $1`, id,
	); err != nil {
		return User{}, fmt.Errorf("reset failed_attempts: %w", err)
	}
	return User{ID: id, Login: dbLogin, CreatedAt: createdAt}, nil
}

// recordFailure increments failed_attempts and applies an exponential backoff
// lockout. Errors are swallowed (logging belongs to the caller) so the original
// authentication error reaches the caller.
func (s *UserService) recordFailure(ctx context.Context, id uuid.UUID, attempts int) {
	lock := lockoutFor(attempts)
	if lock <= 0 {
		_, _ = s.pool.Exec(ctx,
			`UPDATE users SET failed_attempts = $1 WHERE id = $2`, attempts, id)
		return
	}
	until := s.clock().Add(lock)
	_, _ = s.pool.Exec(ctx,
		`UPDATE users SET failed_attempts = $1, locked_until = $2 WHERE id = $3`,
		attempts, until, id)
}

// lockoutFor returns the cooldown duration after `attempts` failed logins.
// 0 means no cooldown yet.
func lockoutFor(attempts int) time.Duration {
	switch {
	case attempts < 5:
		return 0
	case attempts < 10:
		return 30 * time.Second
	case attempts < 20:
		return 5 * time.Minute
	default:
		return 30 * time.Minute
	}
}
