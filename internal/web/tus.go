package web

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/alexnav/storman/internal/audit"
	"github.com/alexnav/storman/internal/rbac"
	"github.com/alexnav/storman/internal/storage"
)

// tusVersion is the only tus protocol revision storman speaks. Aligns with
// the tus-js-client default for compatibility with Uppy.
const tusVersion = "1.0.0"

// tus extensions storman advertises. We support Creation (POST→PATCH flow)
// and Termination (DELETE to abort). Concatenation and Expiration aren't
// implemented; clients gracefully degrade.
const tusExtensions = "creation,termination"

// tusUploadInfo is the JSON shape persisted alongside <id>.bin. Stored next
// to the data file so the upload can resume across process restarts without
// touching the database.
type tusUploadInfo struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	TargetDir string    `json:"target_dir"`
	Filename  string    `json:"filename"`
	Length    int64     `json:"length"`
	CreatedAt time.Time `json:"created_at"`
	MIME      string    `json:"mime,omitempty"`
}

// uploadsRoot is shorthand for the directory where tus parks WIP uploads.
func (s *Server) uploadsRoot() string {
	// dataDir is the parent of meta-storage. Recover it via the FS's trash
	// dir which already lives under meta-storage.
	td := s.FS.TrashDir()
	if td == "" {
		return ""
	}
	// trashDir = <data-dir>/meta-storage/trash → ../uploads
	return filepath.Join(filepath.Dir(td), "uploads")
}

// handleTusOptions advertises capabilities. tus clients call this once on
// startup before issuing POSTs.
func (s *Server) handleTusOptions(w http.ResponseWriter, _ *http.Request) {
	h := w.Header()
	h.Set("Tus-Resumable", tusVersion)
	h.Set("Tus-Version", tusVersion)
	h.Set("Tus-Extension", tusExtensions)
	h.Set("Tus-Max-Size", "10737418240") // 10 GiB cap; tune via config later.
	w.WriteHeader(http.StatusNoContent)
}

// handleTusCreate implements the Creation extension: POST /api/tus with
// Upload-Length and Upload-Metadata (key=base64(value),…). Required metadata:
//
//	filename  — final basename for the file
//	dir       — logical directory the file should land in (default "/")
//
// Returns 201 with Location: /api/tus/<id>.
func (s *Server) handleTusCreate(w http.ResponseWriter, r *http.Request) {
	user := UserFromContext(r.Context())
	if r.Header.Get("Tus-Resumable") != tusVersion {
		tusErr(w, http.StatusPreconditionFailed, "Tus-Resumable header required")
		return
	}
	length, err := strconv.ParseInt(r.Header.Get("Upload-Length"), 10, 64)
	if err != nil || length < 0 {
		tusErr(w, http.StatusBadRequest, "Upload-Length missing or invalid")
		return
	}
	meta := parseTusMetadata(r.Header.Get("Upload-Metadata"))
	filename := meta["filename"]
	dir := meta["dir"]
	if dir == "" {
		dir = "/"
	}
	if filename == "" {
		tusErr(w, http.StatusBadRequest, "metadata 'filename' required")
		return
	}

	// ACL: caller must have Write on the target dir before we let them
	// upload a single byte. Cheap up-front check that mirrors handleWrite.
	parentInfo, err := s.FS.Stat(r.Context(), dir)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := s.Perms.Check(r.Context(), user.ID, parentInfo.ID, rbac.Write); err != nil {
		writeError(w, r, err)
		return
	}

	id, err := tusID()
	if err != nil {
		writeError(w, r, err)
		return
	}
	entry := filepath.Join(s.uploadsRoot(), id)
	if err := os.MkdirAll(entry, 0o700); err != nil {
		writeError(w, r, err)
		return
	}
	dataPath := filepath.Join(entry, "data")
	// Create an empty file pre-allocated to length. truncate avoids a sparse
	// file on filesystems that don't support holes (matters for FAT/ExFAT
	// shares); on APFS/XFS it's free.
	f, err := os.OpenFile(dataPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := f.Truncate(length); err != nil {
		f.Close()
		_ = os.RemoveAll(entry)
		writeError(w, r, err)
		return
	}
	f.Close()

	info := tusUploadInfo{
		ID:        id,
		UserID:    user.ID.String(),
		TargetDir: dir,
		Filename:  filename,
		Length:    length,
		CreatedAt: time.Now().UTC(),
		MIME:      meta["filetype"],
	}
	if err := saveTusInfo(entry, info); err != nil {
		_ = os.RemoveAll(entry)
		writeError(w, r, err)
		return
	}

	w.Header().Set("Tus-Resumable", tusVersion)
	w.Header().Set("Location", "/api/tus/"+id)
	w.WriteHeader(http.StatusCreated)
}

// handleTusHead returns the current Upload-Offset for a resumable upload.
// Tus clients call this whenever they want to resume after a network drop.
func (s *Server) handleTusHead(w http.ResponseWriter, r *http.Request) {
	user := UserFromContext(r.Context())
	id := r.PathValue("id")
	entry, info, err := s.loadTus(id, user.ID.String())
	if err != nil {
		writeError(w, r, err)
		return
	}
	stat, err := os.Stat(filepath.Join(entry, "data"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	// The file is preallocated to `Length`; the actual upload progress is
	// stored alongside in an `offset` file written on every PATCH. Fall
	// back to 0 when the file is missing (fresh creation).
	off := readOffset(entry)
	_ = stat
	w.Header().Set("Tus-Resumable", tusVersion)
	w.Header().Set("Upload-Offset", strconv.FormatInt(off, 10))
	w.Header().Set("Upload-Length", strconv.FormatInt(info.Length, 10))
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
}

// handleTusPatch appends bytes at the requested offset. Requires
//
//	Content-Type: application/offset+octet-stream
//	Upload-Offset: <current offset>
//
// On reaching the declared total length the upload is auto-finalised:
// ImportPath moves the data file into flat-storage and the WIP entry is
// removed.
func (s *Server) handleTusPatch(w http.ResponseWriter, r *http.Request) {
	user := UserFromContext(r.Context())
	id := r.PathValue("id")
	if r.Header.Get("Tus-Resumable") != tusVersion {
		tusErr(w, http.StatusPreconditionFailed, "Tus-Resumable required")
		return
	}
	if r.Header.Get("Content-Type") != "application/offset+octet-stream" {
		tusErr(w, http.StatusUnsupportedMediaType, "Content-Type must be application/offset+octet-stream")
		return
	}
	clientOffset, err := strconv.ParseInt(r.Header.Get("Upload-Offset"), 10, 64)
	if err != nil || clientOffset < 0 {
		tusErr(w, http.StatusBadRequest, "Upload-Offset missing or invalid")
		return
	}

	entry, info, err := s.loadTus(id, user.ID.String())
	if err != nil {
		writeError(w, r, err)
		return
	}
	current := readOffset(entry)
	if clientOffset != current {
		// 409 per tus spec when the client is out of sync.
		w.Header().Set("Tus-Resumable", tusVersion)
		w.Header().Set("Upload-Offset", strconv.FormatInt(current, 10))
		tusErr(w, http.StatusConflict, fmt.Sprintf("offset mismatch (server=%d)", current))
		return
	}

	dataPath := filepath.Join(entry, "data")
	f, err := os.OpenFile(dataPath, os.O_WRONLY, 0o600)
	if err != nil {
		writeError(w, r, err)
		return
	}
	defer f.Close()
	written, err := writeAtFromReader(f, r.Body, current, info.Length-current)
	if err != nil {
		// Persist whatever made it before bailing — the client should
		// resume cleanly via HEAD.
		_ = saveOffset(entry, current+written)
		writeError(w, r, err)
		return
	}
	newOffset := current + written
	if err := saveOffset(entry, newOffset); err != nil {
		writeError(w, r, err)
		return
	}

	if newOffset == info.Length {
		if err := s.finalizeTus(r, entry, info); err != nil {
			writeError(w, r, err)
			return
		}
		s.audit(r.Context(), audit.Event{
			UserID: &user.ID, Action: audit.ActionUpload,
			Result: audit.ResultOK,
			Details: map[string]any{
				"path": joinPathSegments(info.TargetDir, info.Filename), "size": info.Length, "method": "tus",
			},
		})
	}

	w.Header().Set("Tus-Resumable", tusVersion)
	w.Header().Set("Upload-Offset", strconv.FormatInt(newOffset, 10))
	w.WriteHeader(http.StatusNoContent)
}

// handleTusDelete implements the Termination extension. Drops the in-progress
// upload directory. Returns 204 even when the id is unknown (idempotent).
func (s *Server) handleTusDelete(w http.ResponseWriter, r *http.Request) {
	user := UserFromContext(r.Context())
	id := r.PathValue("id")
	entry, _, err := s.loadTus(id, user.ID.String())
	if err == nil {
		_ = os.RemoveAll(entry)
	}
	w.Header().Set("Tus-Resumable", tusVersion)
	w.WriteHeader(http.StatusNoContent)
}

// finalizeTus moves the completed staging file into the FS via ImportPath.
// The WIP directory is removed on success; on failure the entry is left in
// place so the operator can investigate.
func (s *Server) finalizeTus(r *http.Request, entry string, info tusUploadInfo) error {
	dataPath := filepath.Join(entry, "data")
	target := joinPathSegments(info.TargetDir, info.Filename)

	if err := s.FS.ImportPath(r.Context(), target, dataPath); err != nil {
		return err
	}
	if err := os.RemoveAll(entry); err != nil {
		// Sidecar metadata leak isn't catastrophic — log via defer-friendly
		// path. For now keep silent; the cleanup sweeper will catch it.
		_ = err
	}
	return nil
}

// loadTus opens the tus entry directory, parses info.json, and verifies the
// caller matches the original creator. Cross-user access is denied to keep
// authenticated uploads scoped per session.
func (s *Server) loadTus(id, callerID string) (entry string, info tusUploadInfo, err error) {
	if id == "" || strings.ContainsRune(id, os.PathSeparator) {
		return "", tusUploadInfo{}, fmt.Errorf("invalid tus id")
	}
	entry = filepath.Join(s.uploadsRoot(), id)
	info, err = loadTusInfo(entry)
	if err != nil {
		return "", tusUploadInfo{}, err
	}
	if info.UserID != callerID {
		return "", tusUploadInfo{}, rbac.ErrDenied
	}
	return entry, info, nil
}

// --- low-level helpers ---

func tusID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// parseTusMetadata decodes the Upload-Metadata header. Format per tus spec:
//
//	"key1 base64val1,key2 base64val2,..."
//
// Missing values (key with no space) are stored as empty strings.
func parseTusMetadata(h string) map[string]string {
	out := make(map[string]string)
	if h == "" {
		return out
	}
	for _, pair := range strings.Split(h, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		parts := strings.SplitN(pair, " ", 2)
		key := parts[0]
		val := ""
		if len(parts) == 2 {
			if dec, err := base64.StdEncoding.DecodeString(parts[1]); err == nil {
				val = string(dec)
			}
		}
		out[key] = val
	}
	return out
}

func tusErr(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Tus-Resumable", tusVersion)
	writeJSON(w, status, errorBody{Error: msg})
}

func saveTusInfo(entry string, info tusUploadInfo) error {
	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(entry, "info.json"), data, 0o600)
}

func loadTusInfo(entry string) (tusUploadInfo, error) {
	raw, err := os.ReadFile(filepath.Join(entry, "info.json"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// Map to storage.ErrNotFound so writeError gives the client 404.
			return tusUploadInfo{}, fmt.Errorf("%w: tus upload", storage.ErrNotFound)
		}
		return tusUploadInfo{}, err
	}
	var info tusUploadInfo
	return info, json.Unmarshal(raw, &info)
}

// offset is persisted in a tiny sidecar file so HEAD doesn't have to scan
// the data file for the last written byte (sparse holes confuse Stat).
func readOffset(entry string) int64 {
	raw, err := os.ReadFile(filepath.Join(entry, "offset"))
	if err != nil {
		return 0
	}
	n, _ := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	return n
}

func saveOffset(entry string, off int64) error {
	tmp := filepath.Join(entry, "offset.tmp")
	if err := os.WriteFile(tmp, []byte(strconv.FormatInt(off, 10)), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(entry, "offset"))
}

// writeAtFromReader copies up to `max` bytes from r into f starting at the
// given absolute offset. Returns the bytes successfully written.
func writeAtFromReader(f *os.File, r io.Reader, off, max int64) (int64, error) {
	buf := make([]byte, 64*1024)
	var written int64
	for written < max {
		toRead := int64(len(buf))
		if max-written < toRead {
			toRead = max - written
		}
		n, err := r.Read(buf[:toRead])
		if n > 0 {
			if _, werr := f.WriteAt(buf[:n], off+written); werr != nil {
				return written, werr
			}
			written += int64(n)
		}
		if err == io.EOF {
			return written, nil
		}
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

func joinPathSegments(dir, name string) string {
	if dir == "" || dir == "/" {
		return "/" + name
	}
	return strings.TrimRight(dir, "/") + "/" + name
}

