package web

import (
	"context"
	"net/http"
	"time"
)

// readyTimeout bounds the readiness probe (DB ping). Short, because Kubernetes
// failureThreshold compounds — slow probes cascade into long restart loops.
const readyTimeout = 2 * time.Second

// handleHealthz is a pure liveness probe: 200 if the process is alive enough
// to answer HTTP. No external checks here — that's what /readyz is for.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleReadyz runs the configured readiness callback (typically a DB ping).
// Returns 503 with the error message in the body when the callback fails.
// 200 when no callback is configured (test/standalone mode).
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if s.Config.Ready == nil {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
	defer cancel()
	if err := s.Config.Ready(ctx); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready: " + err.Error() + "\n"))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}
