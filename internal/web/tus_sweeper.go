package web

import (
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"
	"time"
)

// StartTusSweeper launches the background loop that purges abandoned tus
// uploads (docs/arch/storage.md "Orphan staging files"). An entry is considered
// abandoned when its info.json mtime is older than retention.
//
// retentionHours<=0 or interval<=0 → no-op (the operator opted out).
func (s *Server) StartTusSweeper(ctx context.Context, retentionHours int, interval time.Duration, logger *log.Logger) {
	if retentionHours <= 0 || interval <= 0 {
		return
	}
	if logger == nil {
		logger = log.Default()
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if n, err := s.sweepTus(time.Duration(retentionHours) * time.Hour); err != nil {
					logger.Printf("tus sweeper: %v", err)
				} else if n > 0 {
					logger.Printf("tus sweeper: purged %d stale uploads", n)
				}
			}
		}
	}()
}

// sweepTus enumerates meta-storage/uploads and removes entries whose
// info.json mtime is older than the retention cutoff. info.json mtime is
// touched implicitly on every PATCH (we rewrite offset adjacent to it), so
// a recently-active upload survives the sweep.
func (s *Server) sweepTus(retention time.Duration) (int, error) {
	root := s.uploadsRoot()
	if root == "" {
		return 0, nil
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	cutoff := time.Now().Add(-retention)
	purged := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		entryDir := filepath.Join(root, e.Name())
		info, err := os.Stat(filepath.Join(entryDir, "offset"))
		if err != nil {
			// Use info.json mtime if there's no offset file yet (POST-only,
			// no PATCH ever happened). Falls back to entry dir mtime when
			// info.json is also missing — that shape is broken anyway.
			info, err = os.Stat(filepath.Join(entryDir, "info.json"))
			if err != nil {
				info, err = os.Stat(entryDir)
				if err != nil {
					continue
				}
			}
		}
		if info.ModTime().After(cutoff) {
			continue
		}
		if err := os.RemoveAll(entryDir); err == nil {
			purged++
		}
	}
	return purged, nil
}
