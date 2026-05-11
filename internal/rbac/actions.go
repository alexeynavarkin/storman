// Package rbac owns the action bitmask and the permission service.
//
// Action layout in the persisted permissions.actions bit(8) column — see
// docs/arch/rbac.md and ADR-0003:
//
//	bit 0 — Read
//	bit 1 — Write
//	bit 2 — Remove
//	bit 3 — Admin
//	bits 4-7 — reserved
//
// Traverse is *not* persisted. It is computed on the fly: if a user has any
// explicit permission on a descendant of node N, they implicitly gain
// Traverse on N and all of N's ancestors, allowing them to walk through to
// the accessible subtree. Internally we surface it as bit 4 (Traverse) of the
// returned mask so callers can check Has(mask, Traverse) uniformly.
package rbac

// Action is a bitmask of permitted operations.
type Action uint8

const (
	Read   Action = 1 << 0
	Write  Action = 1 << 1
	Remove Action = 1 << 2
	Admin  Action = 1 << 3

	// Traverse is computed at evaluation time. It is never stored.
	Traverse Action = 1 << 4
)

// Has reports whether mask grants at least the bits in want.
func Has(mask, want Action) bool {
	return mask&want == want
}
