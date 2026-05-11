package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/alexnav/storman/internal/audit"
	"github.com/alexnav/storman/internal/rbac"
	"github.com/alexnav/storman/internal/storage"
)

// nodeInfoDTO is the wire shape for a node.
type nodeInfoDTO struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Path      string `json:"path"`
	Type      string `json:"type"`
	Size      int64  `json:"size"`
	MIME      string `json:"mime,omitempty"`
	MTime     string `json:"mtime,omitempty"`
	SHA256    string `json:"sha256,omitempty"`
	Children  int    `json:"children,omitempty"`
	Effective uint8  `json:"effective"`
}

func toDTO(n *storage.NodeInfo) nodeInfoDTO {
	d := nodeInfoDTO{
		ID:       n.ID.String(),
		Name:     n.Name,
		Path:     n.Path,
		Type:     string(n.Type),
		Size:     n.Size,
		MIME:     n.MIME,
		Children: n.Children,
	}
	if !n.MTime.IsZero() {
		d.MTime = n.MTime.UTC().Format("2006-01-02T15:04:05Z07:00")
	}
	if len(n.SHA256) > 0 {
		d.SHA256 = fmt.Sprintf("%x", n.SHA256)
	}
	return d
}

// withEffective sets the caller's effective action mask on the DTO. It
// swallows errors — the field stays 0 — because Stat handlers already
// performed an authoritative Check; this number is for UI affordances only.
func (s *Server) withEffective(ctx context.Context, userID uuid.UUID, nodeID uuid.UUID, dto *nodeInfoDTO) {
	mask, err := s.Perms.Effective(ctx, userID, nodeID)
	if err == nil {
		dto.Effective = uint8(mask)
	}
}

func (s *Server) handleStat(w http.ResponseWriter, r *http.Request) {
	user := UserFromContext(r.Context())
	path := r.URL.Query().Get("path")
	info, err := s.FS.Stat(r.Context(), path)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := s.Perms.Check(r.Context(), user.ID, info.ID, rbac.Read); err != nil {
		writeError(w, r, err)
		return
	}
	dto := toDTO(info)
	s.withEffective(r.Context(), user.ID, info.ID, &dto)
	writeJSON(w, http.StatusOK, dto)
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	user := UserFromContext(r.Context())
	path := r.URL.Query().Get("path")
	parentInfo, err := s.FS.Stat(r.Context(), path)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := s.Perms.Check(r.Context(), user.ID, parentInfo.ID, rbac.Read); err != nil {
		writeError(w, r, err)
		return
	}
	entries, err := s.FS.List(r.Context(), path)
	if err != nil {
		writeError(w, r, err)
		return
	}
	out := make([]nodeInfoDTO, 0, len(entries))
	for i := range entries {
		dto := toDTO(&entries[i])
		s.withEffective(r.Context(), user.ID, entries[i].ID, &dto)
		out = append(out, dto)
	}
	writeJSON(w, http.StatusOK, out)
}

type mkdirRequest struct {
	Path    string `json:"path"`
	Parents bool   `json:"parents"`
}

func (s *Server) handleMkdir(w http.ResponseWriter, r *http.Request) {
	user := UserFromContext(r.Context())
	var req mkdirRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid json body"})
		return
	}
	parentPath := parentOf(req.Path)
	parentID, err := s.resolveExistingAncestorID(r.Context(), parentPath, req.Parents)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := s.Perms.Check(r.Context(), user.ID, parentID, rbac.Write); err != nil {
		writeError(w, r, err)
		return
	}
	if err := s.FS.Mkdir(r.Context(), req.Path, storage.MkdirOpts{Parents: req.Parents}); err != nil {
		writeError(w, r, err)
		return
	}
	s.audit(r.Context(), audit.Event{
		UserID:  &user.ID,
		Action:  audit.ActionMkdir,
		NodeID:  &parentID,
		Result:  audit.ResultOK,
		Details: map[string]any{"path": req.Path},
	})
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleRemove(w http.ResponseWriter, r *http.Request) {
	user := UserFromContext(r.Context())
	path := r.URL.Query().Get("path")
	info, err := s.FS.Stat(r.Context(), path)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := s.Perms.Check(r.Context(), user.ID, info.ID, rbac.Remove); err != nil {
		writeError(w, r, err)
		return
	}
	if err := s.FS.Remove(storage.WithActor(r.Context(), user.ID), path); err != nil {
		writeError(w, r, err)
		return
	}
	s.audit(r.Context(), audit.Event{
		UserID:  &user.ID,
		Action:  audit.ActionDelete,
		NodeID:  &info.ID,
		Result:  audit.ResultOK,
		Details: map[string]any{"path": path, "type": string(info.Type)},
	})
	w.WriteHeader(http.StatusNoContent)
}

type renameRequest struct {
	From string `json:"from"`
	To   string `json:"to"`
}

func (s *Server) handleRename(w http.ResponseWriter, r *http.Request) {
	user := UserFromContext(r.Context())
	var req renameRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid json body"})
		return
	}
	oldInfo, err := s.FS.Stat(r.Context(), req.From)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := s.Perms.Check(r.Context(), user.ID, oldInfo.ID, rbac.Write); err != nil {
		writeError(w, r, err)
		return
	}
	newParentID, err := s.resolveExistingAncestorID(r.Context(), parentOf(req.To), false)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := s.Perms.Check(r.Context(), user.ID, newParentID, rbac.Write); err != nil {
		writeError(w, r, err)
		return
	}
	if err := s.FS.Rename(r.Context(), req.From, req.To); err != nil {
		writeError(w, r, err)
		return
	}
	s.audit(r.Context(), audit.Event{
		UserID:  &user.ID,
		Action:  audit.ActionRename,
		NodeID:  &oldInfo.ID,
		Result:  audit.ResultOK,
		Details: map[string]any{"from": req.From, "to": req.To},
	})
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request) {
	user := UserFromContext(r.Context())
	path := r.URL.Query().Get("path")
	info, err := s.FS.Stat(r.Context(), path)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := s.Perms.Check(r.Context(), user.ID, info.ID, rbac.Read); err != nil {
		writeError(w, r, err)
		return
	}
	reader, err := s.FS.OpenRead(r.Context(), path)
	if err != nil {
		writeError(w, r, err)
		return
	}
	defer reader.Close()

	size := reader.Size()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Accept-Ranges", "bytes")

	start, end, ok, badRange := parseRange(r.Header.Get("Range"), size)
	if badRange {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		writeJSON(w, http.StatusRequestedRangeNotSatisfiable, errorBody{Error: "invalid range"})
		return
	}
	if !ok {
		start, end = 0, size-1
	}

	length := end - start + 1
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	if ok && length != size {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
		w.WriteHeader(http.StatusPartialContent)
	}

	section := io.NewSectionReader(reader, start, length)
	if _, err := io.Copy(w, section); err != nil {
		// Connection drop or similar — log via stdlib, client already gone.
		return
	}
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request) {
	user := UserFromContext(r.Context())
	path := r.URL.Query().Get("path")
	parentID, err := s.resolveExistingAncestorID(r.Context(), parentOf(path), false)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := s.Perms.Check(r.Context(), user.ID, parentID, rbac.Write); err != nil {
		writeError(w, r, err)
		return
	}

	expected := r.ContentLength
	writer, err := s.FS.OpenWrite(r.Context(), path, storage.WriteOpts{
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
		UserID:  &user.ID,
		Action:  audit.ActionUpload,
		NodeID:  &parentID,
		Result:  audit.ResultOK,
		Details: map[string]any{"path": path, "size": expected},
	})
	w.WriteHeader(http.StatusNoContent)
}

// streamWrite copies r into w sequentially using WriteAt. The FileWriter API
// is WriteAt because backends use it for resumable patterns; for a one-shot
// PUT we just write at the running offset.
func streamWrite(w storage.FileWriter, r io.Reader) error {
	buf := make([]byte, 64*1024)
	var off int64
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if _, werr := w.WriteAt(buf[:n], off); werr != nil {
				return werr
			}
			off += int64(n)
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// parseRange parses a single "bytes=start-end" header. Returns ok=false if no
// header was sent, ok=true with start/end set, or bad=true on a malformed
// range that the spec says should reply 416.
func parseRange(h string, size int64) (start, end int64, ok bool, bad bool) {
	if h == "" {
		return 0, 0, false, false
	}
	const prefix = "bytes="
	if !strings.HasPrefix(h, prefix) {
		return 0, 0, false, true
	}
	spec := strings.TrimPrefix(h, prefix)
	// Only single ranges supported for MVP — multi-range MIME is rarely used.
	if strings.Contains(spec, ",") {
		return 0, 0, false, true
	}
	dash := strings.IndexByte(spec, '-')
	if dash < 0 {
		return 0, 0, false, true
	}
	startStr := spec[:dash]
	endStr := spec[dash+1:]

	switch {
	case startStr == "" && endStr == "":
		return 0, 0, false, true
	case startStr == "":
		// Suffix range: last N bytes.
		n, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false, true
		}
		if n > size {
			n = size
		}
		return size - n, size - 1, true, false
	case endStr == "":
		s, err := strconv.ParseInt(startStr, 10, 64)
		if err != nil || s < 0 || s >= size {
			return 0, 0, false, true
		}
		return s, size - 1, true, false
	default:
		s, err1 := strconv.ParseInt(startStr, 10, 64)
		e, err2 := strconv.ParseInt(endStr, 10, 64)
		if err1 != nil || err2 != nil || s < 0 || e < s || s >= size {
			return 0, 0, false, true
		}
		if e >= size {
			e = size - 1
		}
		return s, e, true, false
	}
}

// resolveExistingAncestorID returns the node ID of the deepest existing
// ancestor of path. When parents=true and path itself doesn't exist, we walk
// up until we find a real node — that's where the user needs Write perms.
func (s *Server) resolveExistingAncestorID(ctx context.Context, path string, parents bool) (uuid.UUID, error) {
	for {
		info, err := s.FS.Stat(ctx, path)
		if err == nil {
			return info.ID, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return uuid.Nil, err
		}
		if !parents {
			return uuid.Nil, err
		}
		if path == "/" {
			return uuid.Nil, err // shouldn't happen — root always exists post-bootstrap
		}
		path = parentOf(path)
	}
}

// parentOf returns the parent path. parentOf("/a/b") == "/a", parentOf("/a") == "/".
func parentOf(path string) string {
	if path == "" || path == "/" {
		return "/"
	}
	i := strings.LastIndex(path, "/")
	if i <= 0 {
		return "/"
	}
	return path[:i]
}

