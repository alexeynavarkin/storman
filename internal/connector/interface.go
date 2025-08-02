package connector

import (
	"context"
	"io"
	"time"
)

type ConnectorType string

const (
	ConnectorTypeWebdav ConnectorType = "webdav"
)

type Object struct {
	Name string
	Path string

	SizeBytes uint64

	ModifiedTimestamp *time.Time
	CreatedTimestamp  *time.Time
}

type Connector interface {
	Traverse(ctx context.Context, objCh chan Object) error
	Get(ctx context.Context, obj Object) (io.ReadCloser, error)
}
