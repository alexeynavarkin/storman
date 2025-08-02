package index_repo

import (
	"time"
)

type Object struct {
	Path      string
	Name      string
	Extension string

	ContentType string

	SizeBytes uint64

	HashSumSHA512 string

	ModifiedTimestamp *time.Time
	CreatedTimestamp  *time.Time
}

type Index interface {
	Store(obj Object) error
	GetByPath(path string) (*Object, error)
}
