package storage

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestActorRoundTrip(t *testing.T) {
	id := uuid.New()
	ctx := WithActor(context.Background(), id)
	if got := ActorFromContext(ctx); got != id {
		t.Errorf("actor: got %s want %s", got, id)
	}
}

func TestActorDefaultsToNil(t *testing.T) {
	if got := ActorFromContext(context.Background()); got != uuid.Nil {
		t.Errorf("actor default: got %s want %s", got, uuid.Nil)
	}
}
