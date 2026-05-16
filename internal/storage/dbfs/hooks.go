package dbfs

// Test-only fault-injection hooks. In production these are zero-cost: a nil
// check on a struct field, no allocations, no synchronisation. Tests set the
// hook to simulate process crashes between outbox stages and verify that
// RecoverPending heals the resulting state.
//
// Hook points correspond to the durability-critical moments documented in
// docs/arch/storage.md and ADR-0002:
//
//   AfterCreateTx       — tx1 (pending node + outbox) has committed; the
//                         backend writer has not yet been opened. A crash
//                         here leaves a pending node with no on-disk bytes.
//                         RecoverPending must drop the node.
//
//   AfterBackendCommit  — the backend rename has finalised the bytes at the
//                         target path; tx2 has not yet promoted the node to
//                         'ready'. A crash here leaves bytes on disk + a
//                         pending node. RecoverPending must promote them.
//
//   BeforeTrashRename   — Remove's tx is committed (subtree marked deleted);
//                         the physical move into trash has not yet started.
//                         RecoverPending re-runs runTrashOutbox idempotently.
type HookPoint string

const (
	HookAfterCreateTx      HookPoint = "after_create_tx"
	HookAfterBackendCommit HookPoint = "after_backend_commit"
	HookBeforeTrashRename  HookPoint = "before_trash_rename"
)

// HookFn is the user-supplied callback. Returning a non-nil error short-
// circuits the calling op, propagating that error to the caller. The caller
// is responsible for assessing the post-state (typically by reconnecting and
// running RecoverPending).
type HookFn func(point HookPoint) error

// SetTestHook installs a fault-injection callback. Pass nil to clear.
// Test-only — do not call from production code.
func (fs *DBFS) SetTestHook(fn HookFn) { fs.hook = fn }

// fireHook is the internal trigger. Returns nil when no hook is set.
func (fs *DBFS) fireHook(point HookPoint) error {
	if fs.hook == nil {
		return nil
	}
	return fs.hook(point)
}
