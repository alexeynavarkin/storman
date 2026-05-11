package rbac

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Sentinel errors.
var (
	ErrNotFound = errors.New("rbac: not found")
	ErrDenied   = errors.New("rbac: action denied")
)

// PermissionService manages the permissions table and computes effective
// permissions on the fly. ACL cache is intentionally absent in this slice —
// every Effective() call resolves from PostgreSQL. See PLAN.md §2 "Реализация
// RBAC и Наследования".
type PermissionService struct {
	pool *pgxpool.Pool
}

func NewPermissionService(pool *pgxpool.Pool) *PermissionService {
	return &PermissionService{pool: pool}
}

// Grant adds or replaces an explicit permission of user on node with the
// stored action bits (Read/Write/Remove/Admin only — Traverse is rejected).
func (s *PermissionService) Grant(ctx context.Context, nodeID, userID uuid.UUID, actions Action) error {
	if actions&Traverse != 0 {
		return fmt.Errorf("rbac: Traverse is computed, cannot be granted explicitly")
	}
	if actions == 0 {
		return fmt.Errorf("rbac: empty action mask — call Revoke instead")
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO permissions (node_id, user_id, actions)
		 VALUES ($1, $2, ($3::int4)::bit(8))
		 ON CONFLICT (node_id, user_id)
		 DO UPDATE SET actions = EXCLUDED.actions, created_at = now()`,
		nodeID, userID, int32(actions),
	)
	if err != nil {
		return fmt.Errorf("grant: %w", err)
	}
	return nil
}

// ExplicitPermission is a single (user, actions) row attached directly to a
// node. Inherited permissions are not reported by List.
type ExplicitPermission struct {
	UserID  uuid.UUID
	Login   string
	Actions Action
}

// List returns every explicit permission attached to a node, joined with users
// for display purposes. Inherited grants do not appear here.
func (s *PermissionService) List(ctx context.Context, nodeID uuid.UUID) ([]ExplicitPermission, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT p.user_id, u.login, (p.actions)::int
		 FROM permissions p
		 JOIN users u ON u.id = p.user_id
		 WHERE p.node_id = $1
		 ORDER BY u.login`,
		nodeID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]ExplicitPermission, 0)
	for rows.Next() {
		var p ExplicitPermission
		var a int32
		if err := rows.Scan(&p.UserID, &p.Login, &a); err != nil {
			return nil, err
		}
		p.Actions = Action(a) & (Read | Write | Remove | Admin)
		out = append(out, p)
	}
	return out, rows.Err()
}

// Revoke deletes any explicit permission for (node, user).
func (s *PermissionService) Revoke(ctx context.Context, nodeID, userID uuid.UUID) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM permissions WHERE node_id = $1 AND user_id = $2`, nodeID, userID)
	return err
}

// Effective resolves the action mask that user has on node:
//   - the nearest-ancestor (or self) explicit permission contributes its bits;
//   - if user has any explicit permission anywhere in node's subtree (i.e. on
//     a descendant), Traverse is added so they can walk through node to reach it.
//
// Returns 0 if neither source applies.
func (s *PermissionService) Effective(ctx context.Context, userID, nodeID uuid.UUID) (Action, error) {
	var nodePath string
	err := s.pool.QueryRow(ctx,
		`SELECT path::text FROM nodes WHERE id = $1 AND deleted_at IS NULL`, nodeID,
	).Scan(&nodePath)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}

	var ancestorActions int32
	err = s.pool.QueryRow(ctx,
		`SELECT (p.actions)::int FROM permissions p
		 JOIN nodes n ON n.id = p.node_id
		 WHERE p.user_id = $1
		   AND n.path @> $2::ltree
		   AND n.deleted_at IS NULL
		 ORDER BY nlevel(n.path) DESC
		 LIMIT 1`,
		userID, nodePath,
	).Scan(&ancestorActions)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("ancestor lookup: %w", err)
	}

	var hasDescendant bool
	err = s.pool.QueryRow(ctx,
		`SELECT EXISTS(
		   SELECT 1 FROM permissions p
		   JOIN nodes n ON n.id = p.node_id
		   WHERE p.user_id = $1
		     AND n.path <@ $2::ltree
		     AND n.path <> $2::ltree
		     AND n.deleted_at IS NULL
		 )`,
		userID, nodePath,
	).Scan(&hasDescendant)
	if err != nil {
		return 0, fmt.Errorf("descendant lookup: %w", err)
	}

	mask := Action(ancestorActions) & (Read | Write | Remove | Admin)
	if hasDescendant {
		mask |= Traverse
	}
	return mask, nil
}

// Check is a convenience wrapper: returns nil if the user is allowed to
// perform want on nodeID, ErrDenied otherwise. ErrNotFound surfaces if the
// node does not exist.
func (s *PermissionService) Check(ctx context.Context, userID, nodeID uuid.UUID, want Action) error {
	mask, err := s.Effective(ctx, userID, nodeID)
	if err != nil {
		return err
	}
	if !Has(mask, want) {
		return ErrDenied
	}
	return nil
}
