// Package storage defines the storage abstraction layered above the on-disk
// data directory: FileSystem (a single facade over the node tree for all
// frontend protocols) and FileBackend (the storage backend for a single file's
// content). See docs/arch/storage.md and ADR-0001.
package storage

import (
	"time"

	"github.com/google/uuid"
)

// NodeType enumerates node kinds.
type NodeType string

const (
	NodeFile NodeType = "file"
	NodeDir  NodeType = "dir"
)

// WriteMode controls how OpenWrite treats an existing target.
type WriteMode int

const (
	// WriteCreate fails Commit if the target already exists.
	WriteCreate WriteMode = iota
	// WriteOverwrite replaces any existing content; previous content is lost.
	WriteOverwrite
	// WriteModify preserves existing content so the caller can patch it via
	// WriteAt. For backends with content-addressed storage this enables
	// efficient deltas; for FlatFile it materializes a staging copy.
	WriteModify
)

// WriteOpts are the parameters passed to FileSystem.OpenWrite / FileBackend.OpenWrite.
type WriteOpts struct {
	Mode WriteMode
	// ExpectedSize is the upcoming content size; -1 if unknown. Used for
	// quota checks and preallocation hints.
	ExpectedSize int64
}

// MkdirOpts are parameters passed to FileSystem.Mkdir.
type MkdirOpts struct {
	Parents bool
}

// NodeInfo is a lightweight projection of a node (file or directory) suitable
// for protocol-facing code.
type NodeInfo struct {
	ID       uuid.UUID
	Name     string
	Path     string
	Type     NodeType
	Size     int64
	MIME     string
	MTime    time.Time
	SHA256   []byte
	Children int
}

// BackendRef points the FileSystem at the content of a single file inside a
// specific FileBackend. Its layout is opaque to anything above the backend.
type BackendRef struct {
	// Kind matches FileBackend.Name(), e.g. "flat".
	Kind string
	// Data is backend-specific: for "flat" it is a relative path under
	// <storage-dir>; for future "cdc" it would be a manifest id.
	Data string
}

// AllocHint provides context to FileBackend.Allocate so it can choose a
// reasonable backend-specific ref for a new file.
type AllocHint struct {
	NodeID       uuid.UUID
	LogicalPath  string
	ExpectedSize int64
}

// BackendStat is the per-content view exposed by FileBackend.Stat.
type BackendStat struct {
	Size  int64
	MTime time.Time
}
