package web

import (
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/alexnav/storman/internal/audit"
)

// auditRowDTO is the wire shape of an audit_log row.
type auditRowDTO struct {
	ID      int64          `json:"id"`
	TS      string         `json:"ts"`
	UserID  string         `json:"user_id,omitempty"`
	Action  string         `json:"action"`
	NodeID  string         `json:"node_id,omitempty"`
	IP      string         `json:"ip,omitempty"`
	Result  string         `json:"result"`
	Details map[string]any `json:"details,omitempty"`
}

// handleAuditList exposes audit_log entries. Admin-on-root only — the audit
// log is the operator's tool, not a per-user view. Filters are forgiving:
// invalid params are silently ignored so a typo in the query string doesn't
// produce a 400 mid-investigation.
func (s *Server) handleAuditList(w http.ResponseWriter, r *http.Request) {
	caller := UserFromContext(r.Context())
	if err := s.requireRootAdmin(r.Context(), caller.ID); err != nil {
		writeError(w, r, err)
		return
	}
	if s.Audit == nil {
		writeJSON(w, http.StatusOK, []auditRowDTO{})
		return
	}

	q := r.URL.Query()
	filter := audit.Filter{Action: q.Get("action")}
	if v := q.Get("user"); v != "" {
		if id, err := uuid.Parse(v); err == nil {
			filter.UserID = &id
		}
	}
	if v := q.Get("since"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			filter.Since = &t
		}
	}
	if v := q.Get("until"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			filter.Until = &t
		}
	}
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			filter.Limit = n
		}
	}

	rows, err := s.Audit.List(r.Context(), filter)
	if err != nil {
		writeError(w, r, err)
		return
	}
	out := make([]auditRowDTO, 0, len(rows))
	for _, row := range rows {
		dto := auditRowDTO{
			ID:      row.ID,
			TS:      row.TS.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
			Action:  row.Action,
			Result:  row.Result,
			Details: row.Details,
		}
		if row.UserID != nil {
			dto.UserID = row.UserID.String()
		}
		if row.NodeID != nil {
			dto.NodeID = row.NodeID.String()
		}
		if row.IP != nil {
			dto.IP = row.IP.String()
		}
		out = append(out, dto)
	}
	writeJSON(w, http.StatusOK, out)
}
