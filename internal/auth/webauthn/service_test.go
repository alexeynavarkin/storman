package webauthn_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	gowa "github.com/go-webauthn/webauthn/webauthn"

	"github.com/alexnav/storman/internal/auth/webauthn"
	"github.com/alexnav/storman/internal/db/testpg"
)

func newService(t *testing.T) (*webauthn.Service, *pgxpool.Pool) {
	t.Helper()
	pool := testpg.Pool(t)
	svc, err := webauthn.NewService(pool, webauthn.Config{
		RPID:          "localhost",
		RPDisplayName: "Storman test",
		RPOrigins:     []string{"http://localhost"},
		ChallengeTTL:  100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	return svc, pool
}

func createUser(t *testing.T, pool *pgxpool.Pool, login string) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	err := pool.QueryRow(context.Background(),
		`INSERT INTO users (login, password_hash) VALUES ($1, $2) RETURNING id`,
		login, "x",
	).Scan(&id)
	if err != nil {
		t.Fatalf("insert user: %v", err)
	}
	return id
}

func insertChallenge(t *testing.T, pool *pgxpool.Pool, purpose string, userID *uuid.UUID, challengeB64 string, expiresAt time.Time) []byte {
	t.Helper()
	sess := gowa.SessionData{
		Challenge:      challengeB64,
		RelyingPartyID: "localhost",
		Expires:        expiresAt,
	}
	data, err := json.Marshal(sess)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	id, err := base64.RawURLEncoding.DecodeString(challengeB64)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	_, err = pool.Exec(context.Background(),
		`INSERT INTO webauthn_challenges (id, purpose, user_id, session_data, expires_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		id, purpose, userID, data, expiresAt,
	)
	if err != nil {
		t.Fatalf("insert challenge: %v", err)
	}
	return id
}

func TestBeginRegistrationStoresChallenge(t *testing.T) {
	svc, pool := newService(t)
	user := createUser(t, pool, "alice")

	res, err := svc.BeginRegistration(context.Background(), user, "alice")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if len(res.ChallengeID) == 0 || res.Options == nil {
		t.Fatal("empty result")
	}

	var purpose string
	if err := pool.QueryRow(context.Background(),
		`SELECT purpose FROM webauthn_challenges WHERE id = $1`, res.ChallengeID,
	).Scan(&purpose); err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if purpose != "register" {
		t.Errorf("purpose: %q", purpose)
	}
}

func TestChallengeExpiry(t *testing.T) {
	svc, pool := newService(t)
	user := createUser(t, pool, "alice")

	res, err := svc.BeginRegistration(context.Background(), user, "alice")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	_, err = svc.FinishRegistration(context.Background(), user, "alice", res.ChallengeID, "key1", emptyReader{})
	if !errors.Is(err, webauthn.ErrChallengeExpired) {
		t.Fatalf("got %v, want ErrChallengeExpired", err)
	}
}

func TestChallengeSingleUse(t *testing.T) {
	svc, pool := newService(t)
	user := createUser(t, pool, "alice")

	res, err := svc.BeginRegistration(context.Background(), user, "alice")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	// Two concurrent finishers: the row is consumed by the first; the second
	// must see ErrChallengeExpired. The first cannot succeed without a real
	// authenticator response, so it should fail with a non-consume error
	// (parse failure on emptyReader).
	var wg sync.WaitGroup
	results := make([]error, 2)
	for i := range 2 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, results[i] = svc.FinishRegistration(context.Background(), user, "alice", res.ChallengeID, "k", emptyReader{})
		}(i)
	}
	wg.Wait()

	expired := 0
	other := 0
	for _, e := range results {
		if errors.Is(e, webauthn.ErrChallengeExpired) {
			expired++
		} else if e != nil {
			other++
		}
	}
	if expired != 1 || other != 1 {
		t.Fatalf("expected exactly 1 expired + 1 parse error; got expired=%d other=%d (results=%v)", expired, other, results)
	}
}

func TestUserDeleteCascadesCredentials(t *testing.T) {
	svc, pool := newService(t)
	user := createUser(t, pool, "alice")

	_, err := pool.Exec(context.Background(),
		`INSERT INTO webauthn_credentials (user_id, credential_id, public_key, name)
		 VALUES ($1, $2, $3, $4)`,
		user, []byte("cred-id"), []byte("pubkey"), "phone",
	)
	if err != nil {
		t.Fatalf("insert cred: %v", err)
	}

	if _, err := pool.Exec(context.Background(), `DELETE FROM users WHERE id = $1`, user); err != nil {
		t.Fatalf("delete user: %v", err)
	}

	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM webauthn_credentials WHERE user_id = $1`, user,
	).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Errorf("expected cascade delete, %d rows left", n)
	}
	_ = svc
}

func TestRenameUniqueness(t *testing.T) {
	svc, pool := newService(t)
	user := createUser(t, pool, "alice")
	ctx := context.Background()

	var id2 uuid.UUID
	if _, err := pool.Exec(ctx,
		`INSERT INTO webauthn_credentials (user_id, credential_id, public_key, name) VALUES ($1, $2, $3, $4)`,
		user, []byte("c1"), []byte("p1"), "key-A",
	); err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`INSERT INTO webauthn_credentials (user_id, credential_id, public_key, name) VALUES ($1, $2, $3, $4) RETURNING id`,
		user, []byte("c2"), []byte("p2"), "key-B",
	).Scan(&id2); err != nil {
		t.Fatalf("insert 2: %v", err)
	}

	if err := svc.Rename(ctx, user, id2, "key-A"); !errors.Is(err, webauthn.ErrCredentialName) {
		t.Fatalf("expected ErrCredentialName, got %v", err)
	}
	if err := svc.Rename(ctx, user, id2, "key-C"); err != nil {
		t.Fatalf("rename: %v", err)
	}
}

func TestSweepExpired(t *testing.T) {
	svc, pool := newService(t)
	ctx := context.Background()

	expired := insertChallenge(t, pool, "login", nil, "AAAAAAAAAAAAAAAAAAAAAA", time.Now().Add(-time.Hour))
	fresh := insertChallenge(t, pool, "login", nil, "BBBBBBBBBBBBBBBBBBBBBB", time.Now().Add(time.Hour))

	n, err := svc.SweepExpired(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n != 1 {
		t.Errorf("swept %d, want 1", n)
	}

	var c int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM webauthn_challenges WHERE id IN ($1, $2)`, expired, fresh).Scan(&c); err != nil {
		t.Fatalf("count: %v", err)
	}
	if c != 1 {
		t.Errorf("expected 1 remaining, got %d", c)
	}
}

type emptyReader struct{}

func (emptyReader) Read(p []byte) (int, error) { return 0, io.EOF }
