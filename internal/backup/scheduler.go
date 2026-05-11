package backup

import (
	"context"
	"log"
	"time"
)

// Start launches the background backup loop. It returns immediately; the
// loop terminates when ctx is cancelled. Pass interval<=0 to disable.
//
// The first backup runs at `interval` from now (not immediately) — a fresh
// server doesn't need to dump an empty DB. Operators that want one right away
// can invoke `storman backup` manually.
func Start(ctx context.Context, interval time.Duration, retention int, dataDir, dsn string, tools Tools, logger *log.Logger) {
	if interval <= 0 {
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
				path, err := Run(ctx, dataDir, dsn, retention, tools)
				if err != nil {
					logger.Printf("backup: %v", err)
					continue
				}
				logger.Printf("backup: wrote %s", path)
			}
		}
	}()
}
