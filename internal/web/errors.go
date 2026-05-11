package web

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/alexnav/storman/internal/auth"
	"github.com/alexnav/storman/internal/rbac"
	"github.com/alexnav/storman/internal/storage"
)

// errorBody is the JSON shape returned on errors. Stable, machine-readable.
type errorBody struct {
	Error string `json:"error"`
}

// errGone marks resources that existed but are now unreachable (e.g. share
// links past their TTL or use cap). Maps to HTTP 410 Gone.
var errGone = errors.New("gone")

// writeJSON marshals v as JSON with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("web: encode response: %v", err)
	}
}

// writeError maps a service-layer error to an HTTP status + JSON body.
// Unknown errors degrade to 500 with a generic message and are logged.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, storage.ErrNotFound),
		errors.Is(err, rbac.ErrNotFound),
		errors.Is(err, auth.ErrNotFound):
		writeJSON(w, http.StatusNotFound, errorBody{Error: "not found"})
	case errors.Is(err, storage.ErrExists):
		writeJSON(w, http.StatusConflict, errorBody{Error: err.Error()})
	case errors.Is(err, storage.ErrInvalidPath),
		errors.Is(err, auth.ErrWeakPassword):
		writeJSON(w, http.StatusBadRequest, errorBody{Error: err.Error()})
	case errors.Is(err, rbac.ErrDenied):
		writeJSON(w, http.StatusForbidden, errorBody{Error: "denied"})
	case errors.Is(err, auth.ErrPasswordMismatch),
		errors.Is(err, auth.ErrSessionExpired),
		errors.Is(err, auth.ErrAccountLocked):
		writeJSON(w, http.StatusUnauthorized, errorBody{Error: "unauthorized"})
	case errors.Is(err, storage.ErrUnsupported):
		writeJSON(w, http.StatusNotImplemented, errorBody{Error: err.Error()})
	case errors.Is(err, errGone):
		writeJSON(w, http.StatusGone, errorBody{Error: err.Error()})
	default:
		log.Printf("web: unhandled error on %s %s: %v", r.Method, r.URL.Path, err)
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: "internal error"})
	}
}
