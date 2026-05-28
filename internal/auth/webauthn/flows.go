package webauthn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"

	"github.com/go-webauthn/webauthn/protocol"
	gowa "github.com/go-webauthn/webauthn/webauthn"
)

// BeginRegistrationResult bundles what the HTTP layer needs to ship back to
// the browser after the begin step of a registration ceremony.
type BeginRegistrationResult struct {
	Options     *protocol.CredentialCreation
	ChallengeID []byte
}

// BeginRegistration starts the credential creation ceremony for an already
// authenticated user. The caller has verified the session (authedMutate) — we
// trust userID. Existing credentials are loaded so the authenticator can
// refuse to enroll a key that's already registered.
func (s *Service) BeginRegistration(ctx context.Context, userID uuid.UUID, login string) (*BeginRegistrationResult, error) {
	creds, err := s.loadByUser(ctx, userID)
	if err != nil {
		return nil, err
	}
	user := newWAUser(userID, login, creds)

	// AttestationConveyancePreference: none — we don't run an MDS, and
	// self-hosted file storage doesn't need attestation statements.
	opts, sess, err := s.rp.BeginRegistration(user,
		gowa.WithConveyancePreference(protocol.PreferNoAttestation),
	)
	if err != nil {
		return nil, fmt.Errorf("begin registration: %w", err)
	}
	id, err := s.saveChallenge(ctx, purposeRegister, &userID, sess)
	if err != nil {
		return nil, err
	}
	return &BeginRegistrationResult{Options: opts, ChallengeID: id}, nil
}

// FinishRegistration verifies the authenticator's response and persists the
// new credential. The challenge_id must match the user that started the
// ceremony — different user → ErrChallengeExpired (we don't leak the cause).
func (s *Service) FinishRegistration(ctx context.Context, userID uuid.UUID, login string, challengeID []byte, name string, body io.Reader) (Credential, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return Credential{}, errors.New("webauthn: credential name required")
	}
	sess, sessUserID, err := s.consumeChallenge(ctx, challengeID, purposeRegister)
	if err != nil {
		return Credential{}, err
	}
	if sessUserID == nil || *sessUserID != userID {
		return Credential{}, ErrChallengeExpired
	}
	parsed, err := protocol.ParseCredentialCreationResponseBody(body)
	if err != nil {
		return Credential{}, fmt.Errorf("parse credential: %w", err)
	}
	creds, err := s.loadByUser(ctx, userID)
	if err != nil {
		return Credential{}, err
	}
	user := newWAUser(userID, login, creds)
	cred, err := s.rp.CreateCredential(user, *sess, parsed)
	if err != nil {
		return Credential{}, fmt.Errorf("create credential: %w", err)
	}
	return s.insert(ctx, userID, name, cred)
}

// BeginLoginResult bundles the discoverable-login begin step output.
type BeginLoginResult struct {
	Options     *protocol.CredentialAssertion
	ChallengeID []byte
}

// BeginLogin starts a usernameless (discoverable) login ceremony. The
// challenge ID is returned so the browser can submit it alongside the
// assertion; the integrity of the flow comes from the DB-stored challenge,
// not from a CSRF cookie (the user has no session yet).
func (s *Service) BeginLogin(ctx context.Context) (*BeginLoginResult, error) {
	opts, sess, err := s.rp.BeginDiscoverableLogin()
	if err != nil {
		return nil, fmt.Errorf("begin login: %w", err)
	}
	id, err := s.saveChallenge(ctx, purposeLogin, nil, sess)
	if err != nil {
		return nil, err
	}
	return &BeginLoginResult{Options: opts, ChallengeID: id}, nil
}

// FinishLogin verifies the assertion and returns the authenticated user and
// credential row IDs. The caller (HTTP layer) then issues a session cookie via
// the existing SessionService — this package never touches cookies.
//
// Errors:
//   - ErrChallengeExpired: bad/missing/expired challenge ID
//   - ErrNotFound       : asserted credential isn't registered
//   - ErrSignCount      : sign-count regression (possible cloned authenticator)
//   - other            : malformed assertion, signature mismatch, etc.
type FinishLoginResult struct {
	UserID       uuid.UUID
	CredentialID uuid.UUID
}

func (s *Service) FinishLogin(ctx context.Context, challengeID []byte, body io.Reader) (FinishLoginResult, error) {
	sess, _, err := s.consumeChallenge(ctx, challengeID, purposeLogin)
	if err != nil {
		return FinishLoginResult{}, err
	}
	parsed, err := protocol.ParseCredentialRequestResponseBody(body)
	if err != nil {
		return FinishLoginResult{}, fmt.Errorf("parse assertion: %w", err)
	}

	// Resolve the user from the asserted credential. The DiscoverableUserHandler
	// loads the user's full credential set so go-webauthn can verify the
	// signature against the right public key.
	var resolvedUserID uuid.UUID
	var resolvedCredID []byte
	handler := func(rawID, userHandle []byte) (gowa.User, error) {
		userID, cred, err := s.findByCredentialID(ctx, rawID)
		if err != nil {
			return nil, err
		}
		// userHandle, when present, must match the stored user's UUID bytes.
		// gowa already validates this against the credential set, but a
		// defensive check here protects against a misconfigured authenticator.
		if len(userHandle) > 0 && !uuidMatches(userID, userHandle) {
			return nil, ErrNotFound
		}
		resolvedUserID = userID
		resolvedCredID = cred.ID
		return newWAUser(userID, "", []gowa.Credential{cred}), nil
	}

	credential, err := s.rp.ValidateDiscoverableLogin(handler, *sess, parsed)
	if err != nil {
		return FinishLoginResult{}, fmt.Errorf("validate login: %w", err)
	}
	// Look up the row UUID for the credential the assertion matched. We use
	// the raw ID from the validated credential (not the handler's cache) in
	// case go-webauthn picked a different one.
	if err := s.updateAfterLogin(ctx, credential.ID, credential.Authenticator.SignCount); err != nil {
		return FinishLoginResult{}, err
	}
	credUUID, err := s.lookupCredentialRowID(ctx, credential.ID)
	if err != nil {
		return FinishLoginResult{}, err
	}
	_ = resolvedCredID
	return FinishLoginResult{UserID: resolvedUserID, CredentialID: credUUID}, nil
}

// lookupCredentialRowID returns the row id for a credential identified by its
// raw WebAuthn ID. Separate from findByCredentialID because the login flow
// only needs the UUID for audit/response payloads.
func (s *Service) lookupCredentialRowID(ctx context.Context, rawID []byte) (uuid.UUID, error) {
	var id uuid.UUID
	err := s.pool.QueryRow(ctx,
		`SELECT id FROM webauthn_credentials WHERE credential_id = $1`, rawID,
	).Scan(&id)
	return id, err
}

// uuidMatches reports whether handle is the 16-byte representation of id.
func uuidMatches(id uuid.UUID, handle []byte) bool {
	if len(handle) != len(id) {
		return false
	}
	for i := range handle {
		if id[i] != handle[i] {
			return false
		}
	}
	return true
}
