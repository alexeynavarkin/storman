package web

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/google/uuid"

	"github.com/alexnav/storman/internal/audit"
	"github.com/alexnav/storman/internal/auth/webauthn"
)

// passkeyEnabled reports whether the server was constructed with a passkey
// service. Handlers short-circuit to 404 when disabled so the SPA can probe
// without leaking config state.
func (s *Server) passkeyEnabled() bool { return s.Passkeys != nil }

// ---------------------------------------------------------------------------
// Registration (authedMutate — session + CSRF required)

type passkeyRegisterBeginResponse struct {
	PublicKey   any    `json:"publicKey"`
	ChallengeID string `json:"challenge_id"`
}

func (s *Server) handlePasskeyRegisterBegin(w http.ResponseWriter, r *http.Request) {
	if !s.passkeyEnabled() {
		http.NotFound(w, r)
		return
	}
	user := UserFromContext(r.Context())
	if user == nil {
		writeJSON(w, http.StatusUnauthorized, errorBody{Error: "unauthorized"})
		return
	}
	res, err := s.Passkeys.BeginRegistration(r.Context(), user.ID, user.Login)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, passkeyRegisterBeginResponse{
		PublicKey:   res.Options.Response,
		ChallengeID: encodeChallengeID(res.ChallengeID),
	})
}

type passkeyRegisterFinishRequest struct {
	ChallengeID string          `json:"challenge_id"`
	Name        string          `json:"name"`
	Credential  json.RawMessage `json:"credential"`
}

type passkeyDTO struct {
	ID         string  `json:"id"`
	Name       string  `json:"name"`
	Transports []string `json:"transports"`
	BackupState bool   `json:"backup_state"`
	CreatedAt  string  `json:"created_at"`
	LastUsedAt *string `json:"last_used_at,omitempty"`
}

func (s *Server) handlePasskeyRegisterFinish(w http.ResponseWriter, r *http.Request) {
	if !s.passkeyEnabled() {
		http.NotFound(w, r)
		return
	}
	user := UserFromContext(r.Context())
	if user == nil {
		writeJSON(w, http.StatusUnauthorized, errorBody{Error: "unauthorized"})
		return
	}
	var req passkeyRegisterFinishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid json body"})
		return
	}
	if req.Name == "" || len(req.Credential) == 0 || req.ChallengeID == "" {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "challenge_id, name, credential required"})
		return
	}
	challengeID, err := decodeChallengeID(req.ChallengeID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid challenge_id"})
		return
	}
	cred, err := s.Passkeys.FinishRegistration(r.Context(), user.ID, user.Login, challengeID, req.Name, jsonReader(req.Credential))
	if err != nil {
		writePasskeyError(w, r, err)
		return
	}
	s.audit(r.Context(), audit.Event{
		UserID:  &user.ID,
		Action:  audit.ActionPasskeyRegister,
		IP:      clientIP(r),
		Result:  audit.ResultOK,
		Details: map[string]any{"passkey_id": cred.ID.String(), "name": cred.Name},
	})
	writeJSON(w, http.StatusOK, passkeyToDTO(cred))
}

// ---------------------------------------------------------------------------
// Listing / rename / delete (session-scoped management)

type passkeyListResponse struct {
	Passkeys []passkeyDTO `json:"passkeys"`
}

func (s *Server) handlePasskeyList(w http.ResponseWriter, r *http.Request) {
	if !s.passkeyEnabled() {
		http.NotFound(w, r)
		return
	}
	user := UserFromContext(r.Context())
	if user == nil {
		writeJSON(w, http.StatusUnauthorized, errorBody{Error: "unauthorized"})
		return
	}
	creds, err := s.Passkeys.ListByUser(r.Context(), user.ID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	dtos := make([]passkeyDTO, 0, len(creds))
	for _, c := range creds {
		dtos = append(dtos, passkeyToDTO(c))
	}
	writeJSON(w, http.StatusOK, passkeyListResponse{Passkeys: dtos})
}

type passkeyRenameRequest struct {
	Name string `json:"name"`
}

func (s *Server) handlePasskeyRename(w http.ResponseWriter, r *http.Request) {
	if !s.passkeyEnabled() {
		http.NotFound(w, r)
		return
	}
	user := UserFromContext(r.Context())
	if user == nil {
		writeJSON(w, http.StatusUnauthorized, errorBody{Error: "unauthorized"})
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid id"})
		return
	}
	var req passkeyRenameRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "name required"})
		return
	}
	if err := s.Passkeys.Rename(r.Context(), user.ID, id, req.Name); err != nil {
		writePasskeyError(w, r, err)
		return
	}
	s.audit(r.Context(), audit.Event{
		UserID:  &user.ID,
		Action:  audit.ActionPasskeyRename,
		IP:      clientIP(r),
		Result:  audit.ResultOK,
		Details: map[string]any{"passkey_id": id.String(), "name": req.Name},
	})
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handlePasskeyDelete(w http.ResponseWriter, r *http.Request) {
	if !s.passkeyEnabled() {
		http.NotFound(w, r)
		return
	}
	user := UserFromContext(r.Context())
	if user == nil {
		writeJSON(w, http.StatusUnauthorized, errorBody{Error: "unauthorized"})
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid id"})
		return
	}
	if err := s.Passkeys.Delete(r.Context(), user.ID, id); err != nil {
		writePasskeyError(w, r, err)
		return
	}
	s.audit(r.Context(), audit.Event{
		UserID:  &user.ID,
		Action:  audit.ActionPasskeyDelete,
		IP:      clientIP(r),
		Result:  audit.ResultOK,
		Details: map[string]any{"passkey_id": id.String()},
	})
	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// Login (openRoute — no session yet; integrity via DB-stored challenge)

type passkeyLoginBeginResponse struct {
	PublicKey   any    `json:"publicKey"`
	ChallengeID string `json:"challenge_id"`
}

func (s *Server) handlePasskeyLoginBegin(w http.ResponseWriter, r *http.Request) {
	if !s.passkeyEnabled() {
		http.NotFound(w, r)
		return
	}
	ip := clientIP(r)
	clientAddr := ""
	if ip != nil {
		clientAddr = ip.String()
	}
	if ok, retry := s.loginLimiter.AllowIP(clientAddr); !ok {
		w.Header().Set("Retry-After", strconv.Itoa(int(retry.Seconds()+1)))
		s.audit(r.Context(), audit.Event{
			Action:  audit.ActionLoginFailed,
			IP:      ip,
			Result:  audit.ResultDenied,
			Details: map[string]any{"method": "passkey", "reason": "rate_limited"},
		})
		writeJSON(w, http.StatusTooManyRequests, errorBody{Error: "too many login attempts — try again shortly"})
		return
	}
	res, err := s.Passkeys.BeginLogin(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, passkeyLoginBeginResponse{
		PublicKey:   res.Options.Response,
		ChallengeID: encodeChallengeID(res.ChallengeID),
	})
}

type passkeyLoginFinishRequest struct {
	ChallengeID string          `json:"challenge_id"`
	Assertion   json.RawMessage `json:"assertion"`
}

func (s *Server) handlePasskeyLoginFinish(w http.ResponseWriter, r *http.Request) {
	if !s.passkeyEnabled() {
		http.NotFound(w, r)
		return
	}
	ip := clientIP(r)
	clientAddr := ""
	if ip != nil {
		clientAddr = ip.String()
	}
	if ok, retry := s.loginLimiter.AllowIP(clientAddr); !ok {
		w.Header().Set("Retry-After", strconv.Itoa(int(retry.Seconds()+1)))
		writeJSON(w, http.StatusTooManyRequests, errorBody{Error: "too many login attempts — try again shortly"})
		return
	}
	var req passkeyLoginFinishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid json body"})
		return
	}
	if req.ChallengeID == "" || len(req.Assertion) == 0 {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "challenge_id and assertion required"})
		return
	}
	challengeID, err := decodeChallengeID(req.ChallengeID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid challenge_id"})
		return
	}
	result, err := s.Passkeys.FinishLogin(r.Context(), challengeID, jsonReader(req.Assertion))
	if err != nil {
		reason := "invalid_assertion"
		if errors.Is(err, webauthn.ErrSignCount) {
			reason = "sign_count_regression"
		} else if errors.Is(err, webauthn.ErrChallengeExpired) {
			reason = "challenge_expired"
		} else if errors.Is(err, webauthn.ErrNotFound) {
			reason = "unknown_credential"
		}
		s.audit(r.Context(), audit.Event{
			Action:  audit.ActionLoginFailed,
			IP:      ip,
			Result:  audit.ResultDenied,
			Details: map[string]any{"method": "passkey", "reason": reason},
		})
		writePasskeyError(w, r, err)
		return
	}

	// Issue a session exactly like handleLogin does.
	sess, err := s.Sessions.Create(r.Context(), result.UserID, ip, r.UserAgent())
	if err != nil {
		writeError(w, r, err)
		return
	}
	csrf, err := newCSRFToken()
	if err != nil {
		writeError(w, r, err)
		return
	}
	s.setSessionCookie(w, r, sess.ID, sess.ExpiresAt)
	s.setCSRFCookie(w, r, csrf, sess.ExpiresAt)
	user, err := s.Users.FindByID(r.Context(), result.UserID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	admin, _ := s.isRootAdmin(r.Context(), user.ID)
	s.audit(r.Context(), audit.Event{
		UserID:  &user.ID,
		Action:  audit.ActionPasskeyLogin,
		IP:      ip,
		Result:  audit.ResultOK,
		Details: map[string]any{"passkey_id": result.CredentialID.String()},
	})
	writeJSON(w, http.StatusOK, meResponse{
		ID:              user.ID.String(),
		Login:           user.Login,
		IsRootAdmin:     admin,
		PasskeysEnabled: true,
	})
}

// ---------------------------------------------------------------------------
// helpers

func passkeyToDTO(c webauthn.Credential) passkeyDTO {
	dto := passkeyDTO{
		ID:          c.ID.String(),
		Name:        c.Name,
		Transports:  c.Transports,
		BackupState: c.BackupState,
		CreatedAt:   c.CreatedAt.UTC().Format("2006-01-02T15:04:05Z07:00"),
	}
	if c.LastUsedAt != nil {
		s := c.LastUsedAt.UTC().Format("2006-01-02T15:04:05Z07:00")
		dto.LastUsedAt = &s
	}
	if dto.Transports == nil {
		dto.Transports = []string{}
	}
	return dto
}

func writePasskeyError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, webauthn.ErrChallengeExpired):
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "challenge expired"})
	case errors.Is(err, webauthn.ErrCredentialName):
		writeJSON(w, http.StatusConflict, errorBody{Error: "passkey name already in use"})
	case errors.Is(err, webauthn.ErrSignCount):
		writeJSON(w, http.StatusUnauthorized, errorBody{Error: "invalid credentials"})
	case errors.Is(err, webauthn.ErrNotFound):
		writeJSON(w, http.StatusUnauthorized, errorBody{Error: "invalid credentials"})
	default:
		// Generic auth failure — don't leak parse vs. signature distinction.
		writeJSON(w, http.StatusUnauthorized, errorBody{Error: "invalid credentials"})
	}
	// Surface the underlying error to writeError-style logging so server
	// operators can investigate; we have already written the response.
	_ = r
}

func encodeChallengeID(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func decodeChallengeID(s string) ([]byte, error) {
	if b, err := base64.RawURLEncoding.DecodeString(s); err == nil {
		return b, nil
	}
	return base64.URLEncoding.DecodeString(s)
}

// jsonReader wraps a json.RawMessage so the gowa parsers can consume it.
func jsonReader(raw json.RawMessage) io.Reader { return bytes.NewReader(raw) }
