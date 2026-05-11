package storage

import (
	"context"

	"github.com/google/uuid"
)

// actorKey is the private context key under which the calling user's UUID is
// attached for storage operations that need to record provenance (trash
// metadata, audit log). Lives in the storage package to avoid an import cycle
// from dbfs back into web.
type actorKey struct{}

// WithActor attaches the calling user's ID to ctx. Storage operations read it
// via ActorFromContext when they need to record who initiated a change (e.g.
// trash.meta.json's "deleted_by"). The FileSystem interface stays narrow —
// protocols (Web, FTP) inject the actor before calling FS methods.
func WithActor(ctx context.Context, userID uuid.UUID) context.Context {
	return context.WithValue(ctx, actorKey{}, userID)
}

// ActorFromContext returns the user attached via WithActor, or uuid.Nil if
// none. Callers must treat uuid.Nil as "system" (no recorded actor).
func ActorFromContext(ctx context.Context) uuid.UUID {
	if v, ok := ctx.Value(actorKey{}).(uuid.UUID); ok {
		return v
	}
	return uuid.Nil
}
