package web

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/alexnav/storman/internal/audit"
	"github.com/alexnav/storman/internal/auth"
	"github.com/alexnav/storman/internal/rbac"
)

type userDTO struct {
	ID        string `json:"id"`
	Login     string `json:"login"`
	CreatedAt string `json:"created_at,omitempty"`
}

func userToDTO(u auth.User) userDTO {
	return userDTO{
		ID:        u.ID.String(),
		Login:     u.Login,
		CreatedAt: u.CreatedAt.UTC().Format("2006-01-02T15:04:05Z07:00"),
	}
}

func (s *Server) handleUsersList(w http.ResponseWriter, r *http.Request) {
	users, err := s.Users.List(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	out := make([]userDTO, 0, len(users))
	for _, u := range users {
		out = append(out, userToDTO(u))
	}
	writeJSON(w, http.StatusOK, out)
}

// isRootAdmin reports whether userID carries Admin on the tree root. Used both
// to gate user-management endpoints and to flag /api/auth/me responses.
func (s *Server) isRootAdmin(ctx context.Context, userID uuid.UUID) (bool, error) {
	root, err := s.FS.Stat(ctx, "/")
	if err != nil {
		return false, err
	}
	mask, err := s.Perms.Effective(ctx, userID, root.ID)
	if err != nil {
		return false, err
	}
	return rbac.Has(mask, rbac.Admin), nil
}

// requireRootAdmin returns rbac.ErrDenied when the caller is not a root admin.
// Stat / Effective errors propagate so the writer maps them appropriately.
func (s *Server) requireRootAdmin(ctx context.Context, userID uuid.UUID) error {
	ok, err := s.isRootAdmin(ctx, userID)
	if err != nil {
		return err
	}
	if !ok {
		return rbac.ErrDenied
	}
	return nil
}

type createUserRequest struct {
	Login    string `json:"login"`
	Password string `json:"password"`
}

func (s *Server) handleUserCreate(w http.ResponseWriter, r *http.Request) {
	caller := UserFromContext(r.Context())
	if err := s.requireRootAdmin(r.Context(), caller.ID); err != nil {
		writeError(w, r, err)
		return
	}

	var req createUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid json body"})
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
	s.audit(r.Context(), audit.Event{
		UserID:  &caller.ID,
		Action:  audit.ActionUserCreate,
		Result:  audit.ResultOK,
		Details: map[string]any{"new_user_id": user.ID.String(), "login": user.Login},
	})
	writeJSON(w, http.StatusCreated, userToDTO(user))
}

func (s *Server) handleUserDelete(w http.ResponseWriter, r *http.Request) {
	caller := UserFromContext(r.Context())
	targetID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid user id"})
		return
	}
	if targetID == caller.ID {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "cannot delete yourself"})
		return
	}
	if err := s.requireRootAdmin(r.Context(), caller.ID); err != nil {
		writeError(w, r, err)
		return
	}
	if err := s.Users.Delete(r.Context(), targetID); err != nil {
		writeError(w, r, err)
		return
	}
	s.audit(r.Context(), audit.Event{
		UserID:  &caller.ID,
		Action:  audit.ActionUserDelete,
		Result:  audit.ResultOK,
		Details: map[string]any{"deleted_user_id": targetID.String()},
	})
	w.WriteHeader(http.StatusNoContent)
}

type passwordChangeRequest struct {
	Password string `json:"password"`
}

func (s *Server) handleUserPassword(w http.ResponseWriter, r *http.Request) {
	caller := UserFromContext(r.Context())
	targetID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid user id"})
		return
	}
	selfChange := targetID == caller.ID
	if !selfChange {
		if err := s.requireRootAdmin(r.Context(), caller.ID); err != nil {
			writeError(w, r, err)
			return
		}
	}

	var req passwordChangeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid json body"})
		return
	}
	if err := s.Users.UpdatePassword(r.Context(), targetID, req.Password); err != nil {
		writeError(w, r, err)
		return
	}

	if selfChange {
		cookie, cerr := r.Cookie(sessionCookieName)
		keep := ""
		if cerr == nil {
			keep = cookie.Value
		}
		_ = s.Sessions.LogoutAllExcept(r.Context(), targetID, keep)
	} else {
		_ = s.Sessions.LogoutAll(r.Context(), targetID)
	}
	s.audit(r.Context(), audit.Event{
		UserID:  &caller.ID,
		Action:  audit.ActionPasswordReset,
		Result:  audit.ResultOK,
		Details: map[string]any{"target_user_id": targetID.String(), "self": selfChange},
	})
	w.WriteHeader(http.StatusNoContent)
}

type permEntryDTO struct {
	UserID  string `json:"user_id"`
	Login   string `json:"login"`
	Actions uint8  `json:"actions"`
}

type permListResponse struct {
	NodeID    string         `json:"node_id"`
	Effective uint8          `json:"effective_mine"`
	Explicit  []permEntryDTO `json:"explicit"`
}

func (s *Server) handlePermList(w http.ResponseWriter, r *http.Request) {
	user := UserFromContext(r.Context())
	path := r.URL.Query().Get("path")
	info, err := s.FS.Stat(r.Context(), path)
	if err != nil {
		writeError(w, r, err)
		return
	}

	mine, err := s.Perms.Effective(r.Context(), user.ID, info.ID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	// Anyone with Admin on the node (or any ancestor) may inspect the ACL.
	// Without Admin the listing is denied so we don't leak who has access.
	if !rbac.Has(mine, rbac.Admin) {
		writeError(w, r, rbac.ErrDenied)
		return
	}

	entries, err := s.Perms.List(r.Context(), info.ID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	out := permListResponse{
		NodeID:    info.ID.String(),
		Effective: uint8(mine),
		Explicit:  make([]permEntryDTO, 0, len(entries)),
	}
	for _, e := range entries {
		out.Explicit = append(out.Explicit, permEntryDTO{
			UserID:  e.UserID.String(),
			Login:   e.Login,
			Actions: uint8(e.Actions),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

type permGrantRequest struct {
	Path    string `json:"path"`
	UserID  string `json:"user_id"`
	Actions uint8  `json:"actions"`
}

func (s *Server) handlePermGrant(w http.ResponseWriter, r *http.Request) {
	caller := UserFromContext(r.Context())
	var req permGrantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid json body"})
		return
	}
	targetUser, err := uuid.Parse(req.UserID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid user_id"})
		return
	}

	info, err := s.FS.Stat(r.Context(), req.Path)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := s.Perms.Check(r.Context(), caller.ID, info.ID, rbac.Admin); err != nil {
		writeError(w, r, err)
		return
	}
	if err := s.Perms.Grant(r.Context(), info.ID, targetUser, rbac.Action(req.Actions)); err != nil {
		writeError(w, r, err)
		return
	}
	s.audit(r.Context(), audit.Event{
		UserID:  &caller.ID,
		Action:  audit.ActionACLChange,
		NodeID:  &info.ID,
		Result:  audit.ResultOK,
		Details: map[string]any{"op": "grant", "target_user_id": targetUser.String(), "actions": req.Actions, "path": req.Path},
	})
	w.WriteHeader(http.StatusNoContent)
}

type permRevokeRequest struct {
	Path   string `json:"path"`
	UserID string `json:"user_id"`
}

func (s *Server) handlePermRevoke(w http.ResponseWriter, r *http.Request) {
	caller := UserFromContext(r.Context())
	var req permRevokeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid json body"})
		return
	}
	targetUser, err := uuid.Parse(req.UserID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid user_id"})
		return
	}

	info, err := s.FS.Stat(r.Context(), req.Path)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := s.Perms.Check(r.Context(), caller.ID, info.ID, rbac.Admin); err != nil {
		writeError(w, r, err)
		return
	}
	if err := s.Perms.Revoke(r.Context(), info.ID, targetUser); err != nil {
		writeError(w, r, err)
		return
	}
	s.audit(r.Context(), audit.Event{
		UserID:  &caller.ID,
		Action:  audit.ActionACLChange,
		NodeID:  &info.ID,
		Result:  audit.ResultOK,
		Details: map[string]any{"op": "revoke", "target_user_id": targetUser.String(), "path": req.Path},
	})
	w.WriteHeader(http.StatusNoContent)
}
