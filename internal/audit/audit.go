// Package audit implements the append-only audit log: every security- or
// data-relevant action (login, ACL change, upload, delete, …) lands in
// audit_log so an operator can reconstruct what happened. See docs/arch/auth.md.
//
// The service is intentionally best-effort: Log() never returns an error
// because callers shouldn't fail a successful business operation just
// because the audit insert hit a transient DB hiccup. Failures are logged
// to stderr so they're surfaced in monitoring.
package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/netip"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Result enumerates the outcome categories stored in audit_log.result. These
// are stable strings — UIs and queries filter on them.
const (
	ResultOK     = "ok"
	ResultDenied = "denied"
	ResultError  = "error"
)

// Well-known action codes. Keep this list in sync with the per-handler Log
// calls so a follow-up reviewer can grep for the source of any event.
const (
	ActionLogin         = "login"
	ActionLoginFailed   = "login_failed"
	ActionLogout        = "logout"
	ActionUserCreate    = "user_create"
	ActionUserDelete    = "user_delete"
	ActionPasswordReset = "password_reset"
	ActionUpload        = "upload"
	ActionDelete        = "delete"
	ActionRename        = "rename"
	ActionMkdir         = "mkdir"
	ActionRestore       = "trash_restore"
	ActionPurge         = "trash_purge"
	ActionACLChange     = "acl_change"
	ActionShareCreate   = "share_create"
	ActionShareUse      = "share_use"
	ActionBackup        = "backup"
	ActionRecover       = "recover"

	ActionPasskeyRegister = "passkey_register"
	ActionPasskeyLogin    = "passkey_login"
	ActionPasskeyDelete   = "passkey_delete"
	ActionPasskeyRename   = "passkey_rename"

	ActionConfigUpdate = "config_update"
)

// Event is the in-memory shape an event has before it lands in the table.
type Event struct {
	UserID  *uuid.UUID
	Action  string
	NodeID  *uuid.UUID
	IP      *netip.Addr
	Result  string
	Details map[string]any
}

// Row is the projection returned by List for the admin UI.
type Row struct {
	ID      int64
	TS      time.Time
	UserID  *uuid.UUID
	Action  string
	NodeID  *uuid.UUID
	IP      *netip.Addr
	Result  string
	Details map[string]any
}

// Service handles writes and queries against audit_log.
type Service struct {
	pool   *pgxpool.Pool
	logger *log.Logger
}

// NewService wires the service against a pool.
func NewService(pool *pgxpool.Pool, logger *log.Logger) *Service {
	if logger == nil {
		logger = log.Default()
	}
	return &Service{pool: pool, logger: logger}
}

// Log writes the event. Never returns an error — callers in hot paths
// (login, every mutating handler) shouldn't have to thread audit failures
// back to the user.
func (s *Service) Log(ctx context.Context, ev Event) {
	var details []byte
	if len(ev.Details) > 0 {
		if b, err := json.Marshal(ev.Details); err == nil {
			details = b
		}
	}
	var ip *string
	if ev.IP != nil {
		v := ev.IP.String()
		ip = &v
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO audit_log (user_id, action, node_id, ip, result, details)
		 VALUES ($1, $2, $3, $4::inet, $5, $6)`,
		ev.UserID, ev.Action, ev.NodeID, ip, ev.Result, details)
	if err != nil && !errors.Is(err, context.Canceled) {
		s.logger.Printf("audit log insert failed (action=%s result=%s): %v", ev.Action, ev.Result, err)
	}
}

// Filter narrows a List query. Empty fields are ignored.
type Filter struct {
	UserID *uuid.UUID
	Action string
	Since  *time.Time
	Until  *time.Time
	Limit  int
}

// List returns matching events newest-first. A hard default cap of 100 rows
// protects against full-table scans on dashboards.
func (s *Service) List(ctx context.Context, f Filter) ([]Row, error) {
	if f.Limit <= 0 || f.Limit > 1000 {
		f.Limit = 100
	}
	query := `SELECT id, ts, user_id, action, node_id, ip::text, result, details FROM audit_log WHERE 1=1`
	args := []any{}
	idx := 1
	if f.UserID != nil {
		query += fmt.Sprintf(" AND user_id = $%d", idx)
		args = append(args, *f.UserID)
		idx++
	}
	if f.Action != "" {
		query += fmt.Sprintf(" AND action = $%d", idx)
		args = append(args, f.Action)
		idx++
	}
	if f.Since != nil {
		query += fmt.Sprintf(" AND ts >= $%d", idx)
		args = append(args, *f.Since)
		idx++
	}
	if f.Until != nil {
		query += fmt.Sprintf(" AND ts <= $%d", idx)
		args = append(args, *f.Until)
		idx++
	}
	query += fmt.Sprintf(" ORDER BY ts DESC LIMIT $%d", idx)
	args = append(args, f.Limit)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Row, 0, f.Limit)
	for rows.Next() {
		var r Row
		var details []byte
		var ipStr *string
		if err := rows.Scan(&r.ID, &r.TS, &r.UserID, &r.Action, &r.NodeID, &ipStr, &r.Result, &details); err != nil {
			return nil, err
		}
		if ipStr != nil {
			if a, err := netip.ParseAddr(*ipStr); err == nil {
				r.IP = &a
			}
		}
		if len(details) > 0 {
			_ = json.Unmarshal(details, &r.Details)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// keep pgx import live in case the file shrinks during refactors.
var _ = pgx.ErrNoRows
