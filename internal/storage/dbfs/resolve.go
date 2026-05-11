package dbfs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/alexnav/storman/internal/storage"
)

// nodeRow is the projection of nodes that dbfs needs internally.
type nodeRow struct {
	ID          uuid.UUID
	ParentID    *uuid.UUID
	Path        string // ltree as text
	Name        string
	Type        storage.NodeType
	BackendKind *string
	BackendRef  *string
	Size        *int64
	MIME        *string
	MTime       *time.Time
	SHA256      []byte
	Status      string
}

// scanNode reads a nodeRow from a single-row scanner.
type nodeScanner interface {
	Scan(dest ...any) error
}

func scanNode(s nodeScanner) (nodeRow, error) {
	var n nodeRow
	if err := s.Scan(
		&n.ID, &n.ParentID, &n.Path, &n.Name, &n.Type,
		&n.BackendKind, &n.BackendRef, &n.Size, &n.MIME, &n.MTime,
		&n.SHA256, &n.Status,
	); err != nil {
		return nodeRow{}, err
	}
	return n, nil
}

const nodeCols = `id, parent_id, path::text, name, type, backend_kind, backend_ref,
	size, mime, mtime, sha256, status`

// rootNode returns the active root row (parent_id IS NULL).
func (fs *DBFS) rootNode(ctx context.Context, q querier) (nodeRow, error) {
	row := q.QueryRow(ctx,
		`SELECT `+nodeCols+`
		 FROM nodes
		 WHERE parent_id IS NULL AND deleted_at IS NULL`)
	n, err := scanNode(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nodeRow{}, errf(storage.ErrNotFound, "root node missing — run 'storman bootstrap'")
	}
	return n, err
}

// childNode returns the active child of parent by name.
func (fs *DBFS) childNode(ctx context.Context, q querier, parentID uuid.UUID, name string) (nodeRow, error) {
	row := q.QueryRow(ctx,
		`SELECT `+nodeCols+`
		 FROM nodes
		 WHERE parent_id = $1 AND name = $2 AND deleted_at IS NULL`,
		parentID, name)
	n, err := scanNode(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nodeRow{}, storage.ErrNotFound
	}
	return n, err
}

// resolveByPath walks the tree from root to the node at path.
// Returns ErrNotFound if any segment is missing.
func (fs *DBFS) resolveByPath(ctx context.Context, q querier, path string) (nodeRow, error) {
	segments, err := splitPath(path)
	if err != nil {
		return nodeRow{}, err
	}
	cur, err := fs.rootNode(ctx, q)
	if err != nil {
		return nodeRow{}, err
	}
	for _, seg := range segments {
		if cur.Type != storage.NodeDir {
			return nodeRow{}, errf(storage.ErrNotFound, "%q is a file, not a directory", cur.Name)
		}
		child, err := fs.childNode(ctx, q, cur.ID, seg)
		if err != nil {
			return nodeRow{}, fmt.Errorf("resolve %q: %w", seg, err)
		}
		cur = child
	}
	return cur, nil
}

// nodeInfo converts a nodeRow into the public NodeInfo projection.
func (n nodeRow) toInfo(logicalPath string) *storage.NodeInfo {
	info := &storage.NodeInfo{
		ID:   n.ID,
		Name: n.Name,
		Path: logicalPath,
		Type: n.Type,
	}
	if n.Size != nil {
		info.Size = *n.Size
	}
	if n.MIME != nil {
		info.MIME = *n.MIME
	}
	if n.MTime != nil {
		info.MTime = *n.MTime
	}
	if n.SHA256 != nil {
		info.SHA256 = n.SHA256
	}
	return info
}
