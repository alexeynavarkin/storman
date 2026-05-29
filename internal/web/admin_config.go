package web

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"

	"github.com/alexnav/storman/internal/audit"
	"github.com/alexnav/storman/internal/config"
)

// configEnvelope is what the admin settings UI sees on GET and accepts on
// PUT. It mirrors config.Config 1:1 except that:
//   - SecretsKey is hidden (only a "configured" / "missing" indicator is
//     returned), and PUT keeps the existing value if the field is the sentinel
//     SecretsKeySentinel. Sending a new base64 string rewrites it (with all
//     the encrypted-data-loss caveats that implies).
//   - DataDir is read-only at the API layer — Config.validate() requires it
//     to equal the directory containing config.json, and editing it via UI is
//     a footgun.
type configEnvelope struct {
	Cfg               config.Config `json:"config"`
	SecretsKeyPresent bool          `json:"secrets_key_present"`
	RestartPending    bool          `json:"restart_pending"`
}

// SecretsKeySentinel is what the GET returns in Config.SecretsKey instead of
// the real value, and what the PUT recognises as "leave unchanged".
const SecretsKeySentinel = "__unchanged__"

// handleAdminConfigGet returns the current effective config with sensitive
// fields redacted. Admin-only.
func (s *Server) handleAdminConfigGet(w http.ResponseWriter, r *http.Request) {
	user := UserFromContext(r.Context())
	if user == nil {
		writeJSON(w, http.StatusUnauthorized, errorBody{Error: "unauthorized"})
		return
	}
	if ok, _ := s.isRootAdmin(r.Context(), user.ID); !ok {
		writeJSON(w, http.StatusForbidden, errorBody{Error: "admin only"})
		return
	}
	cfg, err := config.Load(s.Config.ConfigPath)
	if err != nil {
		writeError(w, r, err)
		return
	}
	envelope := configEnvelope{
		Cfg:               cfg,
		SecretsKeyPresent: cfg.SecretsKey != "",
		RestartPending:    s.restartPending.Load(),
	}
	envelope.Cfg.SecretsKey = SecretsKeySentinel
	writeJSON(w, http.StatusOK, envelope)
}

// handleAdminConfigPut accepts a full Config payload, validates it, and
// writes it atomically to ConfigPath. The new values take effect on the next
// process start — the response includes restart_pending=true and the SPA
// shows a banner. Admin-only + CSRF (via authedMutate chain).
func (s *Server) handleAdminConfigPut(w http.ResponseWriter, r *http.Request) {
	user := UserFromContext(r.Context())
	if user == nil {
		writeJSON(w, http.StatusUnauthorized, errorBody{Error: "unauthorized"})
		return
	}
	if ok, _ := s.isRootAdmin(r.Context(), user.ID); !ok {
		writeJSON(w, http.StatusForbidden, errorBody{Error: "admin only"})
		return
	}

	var incoming config.Config
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&incoming); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid json body: " + err.Error()})
		return
	}

	current, err := config.Load(s.Config.ConfigPath)
	if err != nil {
		writeError(w, r, err)
		return
	}
	// data_dir is intrinsic to the config file location — overwriting it from
	// the UI would leave the config un-loadable on next start. Force-reset.
	incoming.DataDir = current.DataDir
	// SecretsKey: sentinel → keep current; anything else → rotate (operator
	// took the explicit step of pasting a new key).
	if incoming.SecretsKey == SecretsKeySentinel {
		incoming.SecretsKey = current.SecretsKey
	}

	if err := writeConfigAtomic(s.Config.ConfigPath, incoming); err != nil {
		writeError(w, r, err)
		return
	}
	s.restartPending.Store(true)
	s.audit(r.Context(), audit.Event{
		UserID: &user.ID,
		Action: audit.ActionConfigUpdate,
		IP:     clientIP(r),
		Result: audit.ResultOK,
	})

	// Re-read to confirm what's on disk and to surface defaults that
	// Config.Default would have filled in but the operator may have omitted.
	reloaded, err := config.Load(s.Config.ConfigPath)
	if err != nil {
		writeError(w, r, err)
		return
	}
	envelope := configEnvelope{
		Cfg:               reloaded,
		SecretsKeyPresent: reloaded.SecretsKey != "",
		RestartPending:    true,
	}
	envelope.Cfg.SecretsKey = SecretsKeySentinel
	writeJSON(w, http.StatusOK, envelope)
}

// writeConfigAtomic validates cfg via config.Save semantics (which goes
// through json.Marshal) and writes it via temp-file + rename so a crash mid-
// write can't leave a half-formed file.
//
// We don't call config.Save directly because we want validation BEFORE the
// write — config.Load runs validate() but only after parsing. Use a dry-run
// Marshal/Load roundtrip against a temp path.
func writeConfigAtomic(path string, cfg config.Config) error {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".config.json.*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath) // no-op if rename succeeded

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	// Validate the file Load would see if the server restarted right now.
	if _, err := config.Load(tmpPath); err != nil {
		return errors.New("config validation failed: " + err.Error())
	}

	return os.Rename(tmpPath, path)
}
