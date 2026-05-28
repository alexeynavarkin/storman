package web

import (
	"context"
	"log"
	"time"
)

// StartWebAuthnSweeper purges expired WebAuthn challenge rows on a tick.
// Modeled on StartTusSweeper. No-op when passkeys are disabled or interval<=0.
func (s *Server) StartWebAuthnSweeper(ctx context.Context, interval time.Duration, logger *log.Logger) {
	if s.Passkeys == nil || interval <= 0 {
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
				if n, err := s.Passkeys.SweepExpired(ctx); err != nil {
					logger.Printf("webauthn sweeper: %v", err)
				} else if n > 0 {
					logger.Printf("webauthn sweeper: purged %d expired challenges", n)
				}
			}
		}
	}()
}
