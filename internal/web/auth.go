package web

import (
	"encoding/json"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"time"

	"github.com/alexnav/storman/internal/audit"
)

type loginRequest struct {
	Login    string `json:"login"`
	Password string `json:"password"`
}

type meResponse struct {
	ID          string `json:"id"`
	Login       string `json:"login"`
	IsRootAdmin bool   `json:"is_root_admin"`
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid json body"})
		return
	}
	if req.Login == "" || req.Password == "" {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "login and password required"})
		return
	}

	ip := clientIP(r)
	clientAddr := ""
	if ip != nil {
		clientAddr = ip.String()
	}
	if ok, retry := s.loginLimiter.Allow(req.Login, clientAddr); !ok {
		w.Header().Set("Retry-After", strconv.Itoa(int(retry.Seconds()+1)))
		s.audit(r.Context(), audit.Event{
			Action:  audit.ActionLoginFailed,
			IP:      ip,
			Result:  audit.ResultDenied,
			Details: map[string]any{"login": req.Login, "reason": "rate_limited"},
		})
		writeJSON(w, http.StatusTooManyRequests, errorBody{Error: "too many login attempts — try again shortly"})
		return
	}

	user, err := s.Users.Authenticate(r.Context(), req.Login, req.Password)
	if err != nil {
		// Don't leak whether the login exists — collapse all auth errors to 401.
		s.audit(r.Context(), audit.Event{
			Action:  audit.ActionLoginFailed,
			IP:      ip,
			Result:  audit.ResultDenied,
			Details: map[string]any{"login": req.Login, "reason": err.Error()},
		})
		writeJSON(w, http.StatusUnauthorized, errorBody{Error: "invalid credentials"})
		return
	}

	sess, err := s.Sessions.Create(r.Context(), user.ID, ip, r.UserAgent())
	if err != nil {
		writeError(w, r, err)
		return
	}

	csrf, err := newCSRFToken()
	if err != nil {
		writeError(w, r, err)
		return
	}

	s.setSessionCookie(w, sess.ID, sess.ExpiresAt)
	s.setCSRFCookie(w, csrf, sess.ExpiresAt)
	admin, _ := s.isRootAdmin(r.Context(), user.ID)
	s.audit(r.Context(), audit.Event{
		UserID: &user.ID,
		Action: audit.ActionLogin,
		IP:     ip,
		Result: audit.ResultOK,
	})
	writeJSON(w, http.StatusOK, meResponse{
		ID:          user.ID.String(),
		Login:       user.Login,
		IsRootAdmin: admin,
	})
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	user := UserFromContext(r.Context())
	if cookie, err := r.Cookie(sessionCookieName); err == nil && cookie.Value != "" {
		if err := s.Sessions.Logout(r.Context(), cookie.Value); err != nil {
			writeError(w, r, err)
			return
		}
	}
	s.clearCookie(w, sessionCookieName)
	s.clearCookie(w, csrfCookieName)
	if user != nil {
		s.audit(r.Context(), audit.Event{
			UserID: &user.ID,
			Action: audit.ActionLogout,
			IP:     clientIP(r),
			Result: audit.ResultOK,
		})
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	user := UserFromContext(r.Context())
	if user == nil {
		writeJSON(w, http.StatusUnauthorized, errorBody{Error: "unauthorized"})
		return
	}
	admin, _ := s.isRootAdmin(r.Context(), user.ID)
	writeJSON(w, http.StatusOK, meResponse{
		ID:          user.ID.String(),
		Login:       user.Login,
		IsRootAdmin: admin,
	})
}

func (s *Server) setSessionCookie(w http.ResponseWriter, value string, expires time.Time) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    value,
		Path:     "/",
		Expires:  expires,
		HttpOnly: true,
		Secure:   s.Config.SecureCookies,
		SameSite: http.SameSiteLaxMode,
	})
}

// setCSRFCookie issues the CSRF cookie. It must NOT be HttpOnly — JS in the
// SPA needs to read it to send the X-CSRF-Token header on mutating calls.
func (s *Server) setCSRFCookie(w http.ResponseWriter, value string, expires time.Time) {
	http.SetCookie(w, &http.Cookie{
		Name:     csrfCookieName,
		Value:    value,
		Path:     "/",
		Expires:  expires,
		HttpOnly: false,
		Secure:   s.Config.SecureCookies,
		SameSite: http.SameSiteLaxMode,
	})
}

func (s *Server) clearCookie(w http.ResponseWriter, name string) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: name == sessionCookieName,
		Secure:   s.Config.SecureCookies,
		SameSite: http.SameSiteLaxMode,
	})
}

// clientIP extracts a usable IP for audit/sessions. RemoteAddr is the
// transport peer — for production behind a proxy, callers should plumb the
// forwarded IP through context instead.
func clientIP(r *http.Request) *netip.Addr {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return nil
	}
	if a, err := netip.ParseAddr(host); err == nil {
		return &a
	}
	return nil
}
