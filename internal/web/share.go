package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/alexnav/storman/internal/audit"
	"github.com/alexnav/storman/internal/auth"
	"github.com/alexnav/storman/internal/rbac"
	"github.com/alexnav/storman/internal/storage"
)

// shareLinkDTO is the wire shape of a share_links row. Token is included on
// list responses too — losing the token shouldn't force revoke+recreate.
type shareLinkDTO struct {
	Token     string `json:"token"`
	NodeID    string `json:"node_id"`
	Path      string `json:"path,omitempty"`
	Actions   uint8  `json:"actions"`
	ExpiresAt string `json:"expires_at"`
	MaxUses   *int   `json:"max_uses,omitempty"`
	UsedCount int    `json:"used_count"`
	CreatedBy string `json:"created_by,omitempty"`
	CreatedAt string `json:"created_at"`
	URL       string `json:"url"`
}

type createShareRequest struct {
	Path       string `json:"path"`
	Actions    uint8  `json:"actions"`
	TTLSeconds int    `json:"ttl_seconds"`
	MaxUses    *int   `json:"max_uses,omitempty"`
}

// handleShareCreate issues a new share link. Required scope: Admin on the
// node (or any ancestor) — sharing is a permission delegation and only
// admins of a subtree can delegate. Admin/Traverse bits are stripped from
// the request scope (Admin is rejected upstream in the service).
func (s *Server) handleShareCreate(w http.ResponseWriter, r *http.Request) {
	user := UserFromContext(r.Context())
	var req createShareRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid json body"})
		return
	}
	if req.TTLSeconds <= 0 || req.Actions == 0 {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "ttl_seconds and actions are required"})
		return
	}

	info, err := s.FS.Stat(r.Context(), req.Path)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := s.Perms.Check(r.Context(), user.ID, info.ID, rbac.Admin); err != nil {
		writeError(w, r, err)
		return
	}

	link, err := s.Shares.Create(r.Context(), auth.CreateOpts{
		NodeID:    info.ID,
		Actions:   rbac.Action(req.Actions),
		TTL:       time.Duration(req.TTLSeconds) * time.Second,
		MaxUses:   req.MaxUses,
		CreatedBy: &user.ID,
	})
	if err != nil {
		// Service rejects Admin and other invariants — surface as 400.
		writeJSON(w, http.StatusBadRequest, errorBody{Error: err.Error()})
		return
	}

	s.audit(r.Context(), audit.Event{
		UserID:  &user.ID,
		Action:  audit.ActionShareCreate,
		NodeID:  &info.ID,
		Result:  audit.ResultOK,
		Details: map[string]any{"actions": req.Actions, "ttl_seconds": req.TTLSeconds, "path": req.Path},
	})
	writeJSON(w, http.StatusCreated, toShareDTO(link, req.Path, r))
}

func (s *Server) handleShareListMine(w http.ResponseWriter, r *http.Request) {
	user := UserFromContext(r.Context())
	links, err := s.Shares.ListByCreator(r.Context(), user.ID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, s.linksToDTOs(r, links))
}

// handleShareListAll is the operator view — every active share link, with
// its creator. Admin-on-root only.
func (s *Server) handleShareListAll(w http.ResponseWriter, r *http.Request) {
	caller := UserFromContext(r.Context())
	if err := s.requireRootAdmin(r.Context(), caller.ID); err != nil {
		writeError(w, r, err)
		return
	}
	links, err := s.Shares.ListAll(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, s.linksToDTOs(r, links))
}

// handleShareRevoke deletes a share link. Owner can revoke their own; root
// admins can revoke anyone's. Missing token → 204 (idempotent).
func (s *Server) handleShareRevoke(w http.ResponseWriter, r *http.Request) {
	caller := UserFromContext(r.Context())
	token := r.PathValue("token")
	link, lookupErr := s.Shares.Validate(r.Context(), token, rbac.Action(0))
	// Validate enforces TTL/uses; for revoke we just need to know if it exists
	// and who owns it. ErrShareLinkExpired / ErrShareLinkUsedUp still let us
	// proceed because callers want to clean up dead links too.
	switch {
	case errors.Is(lookupErr, auth.ErrNotFound):
		w.WriteHeader(http.StatusNoContent)
		return
	case lookupErr != nil &&
		!errors.Is(lookupErr, auth.ErrShareLinkExpired) &&
		!errors.Is(lookupErr, auth.ErrShareLinkUsedUp):
		writeError(w, r, lookupErr)
		return
	}
	if link.CreatedBy == nil || *link.CreatedBy != caller.ID {
		// Not the owner — require admin-on-root.
		if err := s.requireRootAdmin(r.Context(), caller.ID); err != nil {
			writeError(w, r, err)
			return
		}
	}
	if err := s.Shares.Revoke(r.Context(), token); err != nil {
		writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// shareInfoDTO is what an anonymous viewer sees when they hit /share/{token}/info.
// Excludes anything that could leak the wider system layout: no creator, no
// internal node id, no full path beyond the basename.
type shareInfoDTO struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Size      int64  `json:"size"`
	MIME      string `json:"mime,omitempty"`
	Actions   uint8  `json:"actions"`
	ExpiresAt string `json:"expires_at"`
}

// handleShareInfo previews a share link without consuming a use. Called by
// the SPA's anonymous viewer page to render a "Download" / "Upload" UI.
func (s *Server) handleShareInfo(w http.ResponseWriter, r *http.Request) {
	link, err := s.Shares.Validate(r.Context(), r.PathValue("token"), rbac.Action(0))
	if err != nil {
		writeError(w, r, mapShareErr(err))
		return
	}
	info, err := s.statByID(r.Context(), link.NodeID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, shareInfoDTO{
		Name:      info.Name,
		Type:      string(info.Type),
		Size:      info.Size,
		MIME:      info.MIME,
		Actions:   link.Actions,
		ExpiresAt: link.ExpiresAt.UTC().Format(time.RFC3339),
	})
}

// handleShareDownload streams the file content. Each call consumes one use.
// Refuses when the scope doesn't include Read or the node is a directory.
func (s *Server) handleShareDownload(w http.ResponseWriter, r *http.Request) {
	token := r.PathValue("token")
	link, err := s.Shares.Use(r.Context(), token, rbac.Read)
	if err != nil {
		writeError(w, r, mapShareErr(err))
		return
	}
	info, err := s.statByID(r.Context(), link.NodeID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if info.Type != storage.NodeFile {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "share link points to a directory — use /upload for STOR"})
		return
	}

	reader, err := s.FS.OpenRead(r.Context(), info.Path)
	if err != nil {
		writeError(w, r, err)
		return
	}
	defer reader.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	if info.MIME != "" {
		w.Header().Set("Content-Type", info.MIME)
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename=%q`, info.Name))
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
	w.Header().Set("Cache-Control", "no-store")
	sec := io.NewSectionReader(reader, 0, info.Size)
	if _, err := io.Copy(w, sec); err != nil {
		return
	}
	s.audit(r.Context(), audit.Event{
		Action:  audit.ActionShareUse,
		NodeID:  &info.ID,
		IP:      clientIP(r),
		Result:  audit.ResultOK,
		Details: map[string]any{"op": "download", "token": token[:8] + "…"},
	})
}

// handleShareUpload writes a new file into the shared directory. Requires
// Write in the link's scope; rejected if the link targets a file (no in-place
// overwrite — anonymous uploads always create new files).
func (s *Server) handleShareUpload(w http.ResponseWriter, r *http.Request) {
	token := r.PathValue("token")
	name := r.URL.Query().Get("name")
	if name == "" {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "?name=<filename> required"})
		return
	}
	link, err := s.Shares.Use(r.Context(), token, rbac.Write)
	if err != nil {
		writeError(w, r, mapShareErr(err))
		return
	}
	dir, err := s.statByID(r.Context(), link.NodeID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if dir.Type != storage.NodeDir {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "share link must target a directory for uploads"})
		return
	}

	target := dir.Path
	if target != "/" {
		target += "/"
	}
	target += name

	expected := r.ContentLength
	writer, err := s.FS.OpenWrite(r.Context(), target, storage.WriteOpts{
		Mode:         storage.WriteCreate,
		ExpectedSize: expected,
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := streamWrite(writer, r.Body); err != nil {
		_ = writer.Abort()
		_ = writer.Close()
		writeError(w, r, err)
		return
	}
	if err := writer.Commit(); err != nil {
		_ = writer.Close()
		writeError(w, r, err)
		return
	}
	if err := writer.Close(); err != nil && !errors.Is(err, storage.ErrUnfinalized) {
		writeError(w, r, err)
		return
	}
	s.audit(r.Context(), audit.Event{
		Action:  audit.ActionShareUse,
		NodeID:  &dir.ID,
		IP:      clientIP(r),
		Result:  audit.ResultOK,
		Details: map[string]any{"op": "upload", "token": token[:8] + "…", "name": name},
	})
	w.WriteHeader(http.StatusNoContent)
}

// statByID resolves a node from its UUID. share_links references nodes by id,
// but FS.OpenRead/Stat want logical paths — bridge by reconstructing the
// node's path through the pool, then handing it to FS.Stat so caches and ACL
// invariants are reused.
func (s *Server) statByID(ctx context.Context, id uuid.UUID) (*storage.NodeInfo, error) {
	var logical string
	err := s.Users.Pool().QueryRow(ctx,
		`WITH RECURSIVE chain AS (
		   SELECT id, parent_id, name, 0 AS depth
		     FROM nodes WHERE id = $1 AND deleted_at IS NULL
		   UNION ALL
		   SELECT n.id, n.parent_id, n.name, c.depth + 1
		     FROM nodes n
		     JOIN chain c ON c.parent_id = n.id
		    WHERE n.deleted_at IS NULL
		 )
		 SELECT COALESCE('/' || string_agg(name, '/' ORDER BY depth DESC), '/')
		 FROM chain
		 WHERE name <> ''`, id).Scan(&logical)
	if err != nil {
		return nil, fmt.Errorf("locate node %s: %w", id, err)
	}
	if logical == "" {
		logical = "/"
	}
	return s.FS.Stat(ctx, logical)
}

func toShareDTO(link auth.ShareLink, path string, r *http.Request) shareLinkDTO {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	dto := shareLinkDTO{
		Token:     link.Token,
		NodeID:    link.NodeID.String(),
		Path:      path,
		Actions:   link.Actions,
		ExpiresAt: link.ExpiresAt.UTC().Format(time.RFC3339),
		MaxUses:   link.MaxUses,
		UsedCount: link.UsedCount,
		CreatedAt: link.CreatedAt.UTC().Format(time.RFC3339),
		URL:       fmt.Sprintf("%s://%s/share/%s/info", scheme, r.Host, link.Token),
	}
	if link.CreatedBy != nil {
		dto.CreatedBy = link.CreatedBy.String()
	}
	return dto
}

func (s *Server) linksToDTOs(r *http.Request, links []auth.ShareLink) []shareLinkDTO {
	out := make([]shareLinkDTO, 0, len(links))
	for _, l := range links {
		out = append(out, toShareDTO(l, "", r))
	}
	return out
}

// mapShareErr translates share-link sentinels into HTTP-appropriate errors
// the existing writeError function understands.
func mapShareErr(err error) error {
	switch {
	case errors.Is(err, auth.ErrNotFound):
		return storage.ErrNotFound
	case errors.Is(err, auth.ErrShareLinkExpired):
		return fmt.Errorf("%w: share link expired", errGone)
	case errors.Is(err, auth.ErrShareLinkUsedUp):
		return fmt.Errorf("%w: share link reached max uses", errGone)
	case errors.Is(err, auth.ErrShareLinkScope):
		return rbac.ErrDenied
	}
	return err
}
