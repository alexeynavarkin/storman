package web

import (
	"net/http"

	"github.com/google/uuid"

	"github.com/alexnav/storman/internal/audit"
)

// trashEntryDTO is the wire shape for a trash entry. Mirrors dbfs.TrashEntry
// with strings instead of uuid.UUID for JSON ergonomics.
type trashEntryDTO struct {
	TrashUUID       string `json:"trash_uuid"`
	RootNodeID      string `json:"root_node_id"`
	RootLogicalPath string `json:"root_logical_path"`
	RootNodeType    string `json:"root_node_type"`
	DeletedBy       string `json:"deleted_by,omitempty"`
	DeletedAt       string `json:"deleted_at"`
	SizeBytes       int64  `json:"size_bytes"`
	CanRestore      bool   `json:"can_restore"`
	CanPurge        bool   `json:"can_purge"`
}

// handleTrashList returns every trash entry. Gated to root admins only:
// live ACL on the soft-deleted root node returns nothing (rbac.Effective
// skips deleted_at IS NOT NULL rows), so we can't reuse the regular Remove
// permission. Root admin is a coarser but correct fallback for the MVP.
func (s *Server) handleTrashList(w http.ResponseWriter, r *http.Request) {
	caller := UserFromContext(r.Context())
	if err := s.requireRootAdmin(r.Context(), caller.ID); err != nil {
		writeError(w, r, err)
		return
	}
	entries, err := s.FS.ListTrash(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	out := make([]trashEntryDTO, 0, len(entries))
	for _, e := range entries {
		out = append(out, trashEntryDTO{
			TrashUUID:       e.TrashUUID.String(),
			RootNodeID:      e.RootNodeID.String(),
			RootLogicalPath: e.RootLogicalPath,
			RootNodeType:    e.RootNodeType,
			DeletedBy:       uuidStringOrEmpty(e.DeletedBy),
			DeletedAt:       e.DeletedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
			SizeBytes:       e.SizeBytes,
			CanRestore:      true,
			CanPurge:        true,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func uuidStringOrEmpty(id uuid.UUID) string {
	if id == uuid.Nil {
		return ""
	}
	return id.String()
}

func (s *Server) handleTrashRestore(w http.ResponseWriter, r *http.Request) {
	caller := UserFromContext(r.Context())
	if err := s.requireRootAdmin(r.Context(), caller.ID); err != nil {
		writeError(w, r, err)
		return
	}
	trashUUID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid trash id"})
		return
	}
	if err := s.FS.Restore(r.Context(), trashUUID); err != nil {
		writeError(w, r, err)
		return
	}
	s.audit(r.Context(), audit.Event{
		UserID:  &caller.ID,
		Action:  audit.ActionRestore,
		Result:  audit.ResultOK,
		Details: map[string]any{"trash_uuid": trashUUID.String()},
	})
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleTrashPurge(w http.ResponseWriter, r *http.Request) {
	caller := UserFromContext(r.Context())
	if err := s.requireRootAdmin(r.Context(), caller.ID); err != nil {
		writeError(w, r, err)
		return
	}
	trashUUID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid trash id"})
		return
	}
	if err := s.FS.Purge(r.Context(), trashUUID); err != nil {
		writeError(w, r, err)
		return
	}
	s.audit(r.Context(), audit.Event{
		UserID:  &caller.ID,
		Action:  audit.ActionPurge,
		Result:  audit.ResultOK,
		Details: map[string]any{"trash_uuid": trashUUID.String()},
	})
	w.WriteHeader(http.StatusNoContent)
}
