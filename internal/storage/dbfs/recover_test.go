package dbfs_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/alexnav/storman/internal/storage"
)

// TestRecoverPendingIsIdempotent: calling RecoverPending twice in a row must
// be a no-op the second time. Builds on the existing recover-promotes test
// but also verifies the outbox has been archived (not just the node promoted).
func TestRecoverPendingIsIdempotent(t *testing.T) {
	f, ctx := newFS(t)

	// Drive a write through Commit, then forcibly resurrect the outbox row
	// and demote the node to 'pending' — this is what the on-disk state
	// would look like if the process died between backend.Commit() and tx2.
	w, err := f.fs.OpenWrite(ctx, "/idem.txt", storage.WriteOpts{Mode: storage.WriteCreate})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteAt([]byte("payload"), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}

	if _, err := f.pool.Exec(ctx, `UPDATE nodes SET status='pending' WHERE name='idem.txt'`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `
		INSERT INTO outbox (op, node_id, payload, status)
		SELECT 'create_file', id,
		       jsonb_build_object('node_id', id, 'backend_kind', backend_kind, 'backend_ref', backend_ref),
		       'pending'
		FROM nodes WHERE name='idem.txt'`); err != nil {
		t.Fatal(err)
	}

	acted, err := f.fs.RecoverPending(ctx)
	if err != nil {
		t.Fatalf("first recover: %v", err)
	}
	if acted != 1 {
		t.Fatalf("first acted=%d (want 1)", acted)
	}

	// Second call must find nothing left to do.
	acted2, err := f.fs.RecoverPending(ctx)
	if err != nil {
		t.Fatalf("second recover: %v", err)
	}
	if acted2 != 0 {
		t.Errorf("second acted=%d (want 0)", acted2)
	}

	// Outbox is empty for this node — archive happened.
	var pending int
	if err := f.pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox o JOIN nodes n ON n.id = o.node_id WHERE n.name='idem.txt'`,
	).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 0 {
		t.Errorf("outbox not archived: %d rows remain", pending)
	}

	// Node is ready and content readable.
	info, err := f.fs.Stat(ctx, "/idem.txt")
	if err != nil {
		t.Fatalf("Stat after recover: %v", err)
	}
	if info.Size != int64(len("payload")) {
		t.Errorf("size: %d", info.Size)
	}
}

// TestRecoverTrashIsIdempotent: rerun runTrashOutbox on an already-processed
// subtree (payload moved, meta written) should be a clean no-op via
// RecoverPending — the trash row stays archived once and only once.
func TestRecoverTrashIsIdempotent(t *testing.T) {
	f, ctx := newFS(t)

	writeFile(t, f.fs, "/dropme.txt", []byte("bye"))
	if err := f.fs.Remove(ctx, "/dropme.txt"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	// runTrashOutbox already ran synchronously inside Remove. Forge a second
	// pending row pointing at the same already-moved payload to simulate the
	// "crashed mid-rename, came back up, payload is already in trash" case.
	entries, err := f.fs.ListTrash(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("trash entries: %d", len(entries))
	}
	uuid := entries[0].TrashUUID

	if _, err := f.pool.Exec(ctx, `
		INSERT INTO outbox (op, node_id, payload, status)
		VALUES ('trash', $1::uuid,
		    jsonb_build_object(
		        'trash_uuid', $2::text,
		        'root_node_id', $3::text,
		        'root_logical_path', '/dropme.txt',
		        'root_node_type', 'file',
		        'root_backend_kind', 'flat',
		        'root_backend_ref', 'dropme.txt',
		        'deleted_by', '00000000-0000-0000-0000-000000000000',
		        'deleted_at', now()
		    ),
		    'pending')`,
		entries[0].RootNodeID, uuid.String(), entries[0].RootNodeID.String()); err != nil {
		t.Fatal(err)
	}

	acted, err := f.fs.RecoverPending(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if acted != 1 {
		t.Errorf("acted: %d (want 1 — the forged row should be archived)", acted)
	}

	// Trash entry intact, payload still present.
	payload := filepath.Join(f.trashDir, uuid.String(), "payload")
	if data, err := os.ReadFile(payload); err != nil {
		t.Errorf("payload missing after idempotent recover: %v", err)
	} else if string(data) != "bye" {
		t.Errorf("payload contents drifted: %q", data)
	}

	// Outbox empty again.
	var pending int
	if err := f.pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox WHERE op='trash'`,
	).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 0 {
		t.Errorf("outbox: %d trash rows remain", pending)
	}
}

