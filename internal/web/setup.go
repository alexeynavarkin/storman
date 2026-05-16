package web

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/alexnav/storman/internal/audit"
	"github.com/alexnav/storman/internal/auth"
	"github.com/alexnav/storman/internal/rbac"
)

// setupState holds the one-time token printed to the operator when the users
// table is empty. The token authenticates the very first POST /api/setup/admin
// call so an attacker who reaches the unauthenticated endpoint before the
// operator cannot hijack the instance.
type setupState struct {
	mu    sync.Mutex
	token string
}

// InitSetup probes the users table and, if empty, generates and logs a fresh
// setup token. Idempotent across restarts: every fresh process generates a new
// token until the first admin is created. Once at least one user exists the
// function is a no-op.
func (s *Server) InitSetup(ctx context.Context) error {
	n, err := s.Users.Count(ctx)
	if err != nil {
		return fmt.Errorf("setup: count users: %w", err)
	}
	if n > 0 {
		return nil
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return fmt.Errorf("setup: generate token: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	s.setup.mu.Lock()
	s.setup.token = token
	s.setup.mu.Unlock()
	slog.Warn("first-run setup required — open /setup in a browser and paste the token below; regenerated on every restart until the first admin is created",
		"setup_token", token)
	return nil
}

type setupStateDTO struct {
	NeedsSetup bool `json:"needs_setup"`
}

func (s *Server) handleSetupState(w http.ResponseWriter, r *http.Request) {
	n, err := s.Users.Count(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	if n > 0 {
		s.clearSetupToken()
	}
	writeJSON(w, http.StatusOK, setupStateDTO{NeedsSetup: n == 0})
}

type setupAdminReq struct {
	Token    string `json:"token"`
	Login    string `json:"login"`
	Password string `json:"password"`
}

type setupAdminResp struct {
	Login string `json:"login"`
}

func (s *Server) handleSetupAdmin(w http.ResponseWriter, r *http.Request) {
	n, err := s.Users.Count(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	if n > 0 {
		s.clearSetupToken()
		writeJSON(w, http.StatusGone, errorBody{Error: "setup already completed"})
		return
	}

	s.setup.mu.Lock()
	have := s.setup.token
	s.setup.mu.Unlock()
	if have == "" {
		writeJSON(w, http.StatusGone, errorBody{Error: "setup token not initialised — restart the server to issue a new one"})
		return
	}

	var req setupAdminReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid json body"})
		return
	}
	req.Token = strings.TrimSpace(req.Token)
	req.Login = strings.TrimSpace(req.Login)
	if subtle.ConstantTimeCompare([]byte(req.Token), []byte(have)) != 1 {
		s.audit(r.Context(), audit.Event{
			Action:  audit.ActionLoginFailed,
			Result:  audit.ResultDenied,
			Details: map[string]any{"reason": "setup-token-mismatch"},
		})
		writeJSON(w, http.StatusUnauthorized, errorBody{Error: "invalid setup token"})
		return
	}

	user, err := s.Users.Create(r.Context(), req.Login, req.Password)
	if err != nil {
		if errors.Is(err, auth.ErrLoginTaken) {
			writeJSON(w, http.StatusConflict, errorBody{Error: err.Error()})
			return
		}
		writeError(w, r, err)
		return
	}

	root, err := s.FS.Stat(r.Context(), "/")
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := s.Perms.Grant(r.Context(), root.ID, user.ID, rbac.Read|rbac.Write|rbac.Remove|rbac.Admin); err != nil {
		writeError(w, r, err)
		return
	}

	s.clearSetupToken()
	s.audit(r.Context(), audit.Event{
		UserID:  &user.ID,
		Action:  audit.ActionUserCreate,
		Result:  audit.ResultOK,
		Details: map[string]any{"via": "setup-wizard", "login": user.Login, "new_user_id": user.ID.String()},
	})
	writeJSON(w, http.StatusCreated, setupAdminResp{Login: user.Login})
}

func (s *Server) clearSetupToken() {
	s.setup.mu.Lock()
	s.setup.token = ""
	s.setup.mu.Unlock()
}

// SetupTokenForTest returns the current setup token. Intended for tests only.
func (s *Server) SetupTokenForTest() string {
	s.setup.mu.Lock()
	defer s.setup.mu.Unlock()
	return s.setup.token
}
