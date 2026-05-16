// WebDAV interface (see docs/arch/interfaces.md). Mounted under the
// main HTTPS server at a configurable prefix (default "/dav"). Auth: HTTP
// Basic with an app-password — the same secret used for FTPS. Sessions and
// CSRF are bypassed; instead each request authenticates fresh, with rate
// limiting on failed attempts via the shared loginLimiter.
//
// Locks: in-memory via webdav.NewMemLS — not safe across replicas. Fine for
// the single-node MVP; replace with a Postgres-backed LockSystem when we
// scale out.

package web

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"

	"golang.org/x/net/webdav"

	"github.com/alexnav/storman/internal/audit"
	"github.com/alexnav/storman/internal/storage"
)

// davHandler builds the webdav.Handler + Basic-auth wrapper. Called from
// routes() when davEnabled is true.
func (s *Server) davHandler() http.Handler {
	h := &webdav.Handler{
		Prefix:     s.davPrefix,
		FileSystem: &davFS{fs: s.FS, perms: s.Perms, auditFn: s.audit},
		LockSystem: webdav.NewMemLS(),
		Logger: func(r *http.Request, err error) {
			if err != nil {
				slog.Warn("webdav request failed",
					"method", r.Method,
					"path", r.URL.Path,
					"err", err)
			}
		},
	}
	return s.davAuth(h)
}

// davAuth enforces HTTP Basic auth against an app-password, plumbs the user
// into the request context for downstream FS calls, and emits an audit event
// on failed authentication.
func (s *Server) davAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		login, secret, ok := r.BasicAuth()
		if !ok {
			s.davChallenge(w, http.StatusUnauthorized, "auth required")
			return
		}
		ip := clientIP(r)
		ipStr := ""
		if ip != nil {
			ipStr = ip.String()
		}

		// Cache hit: skip the rate limiter and the DB-backed authenticator.
		// macOS Finder issues many parallel WebDAV requests per user action,
		// each with the same Basic credentials — without this cache the chatty
		// pattern would exhaust the 10/min/login bucket within seconds and
		// rip apart multi-step sequences like LOCK → MOVE → UNLOCK.
		cacheKey := davAuthKey(login, secret, ipStr)
		if cached, ok := s.davAuthCache.Get(cacheKey); ok {
			ctx := context.WithValue(r.Context(), ctxKeyUser{}, &cached)
			ctx = storage.WithActor(ctx, cached.ID)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		if allow, retry := s.loginLimiter.Allow(login, ipStr); !allow {
			w.Header().Set("Retry-After", strconv.Itoa(int(retry.Seconds()+1)))
			// Suppress the audit if this login has a recent successful auth
			// cached: in that case the 429 is just Finder's burst exceeding
			// the bucket with valid credentials, not a real attack signal.
			if !s.davAuthCache.HasLogin(login) {
				s.audit(r.Context(), audit.Event{
					Action:  audit.ActionLoginFailed,
					IP:      ip,
					Result:  audit.ResultDenied,
					Details: map[string]any{"login": login, "reason": "rate_limited", "channel": "webdav"},
				})
			}
			http.Error(w, "too many auth attempts — try again shortly", http.StatusTooManyRequests)
			return
		}
		user, err := s.Users.AuthenticateAppPassword(r.Context(), login, secret)
		if err != nil {
			s.audit(r.Context(), audit.Event{
				Action:  audit.ActionLoginFailed,
				IP:      ip,
				Result:  audit.ResultDenied,
				Details: map[string]any{"login": login, "reason": err.Error(), "channel": "webdav"},
			})
			s.davChallenge(w, http.StatusUnauthorized, "invalid credentials")
			return
		}
		s.davAuthCache.Put(cacheKey, user)
		ctx := context.WithValue(r.Context(), ctxKeyUser{}, &user)
		ctx = storage.WithActor(ctx, user.ID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) davChallenge(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("WWW-Authenticate", `Basic realm="storman webdav"`)
	http.Error(w, msg, status)
}

// SetDav configures the WebDAV interface and rebuilds the handler. Idempotent
// — call again with enabled=false to disable. Prefix must start with "/" and
// not end with "/"; pass "" to use the default "/dav".
func (s *Server) SetDav(enabled bool, prefix string) *Server {
	if prefix == "" {
		prefix = "/dav"
	}
	s.davEnabled = enabled
	s.davPrefix = prefix
	s.Handler = s.routes()
	return s
}
