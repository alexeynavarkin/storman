package webauthn

import (
	gowa "github.com/go-webauthn/webauthn/webauthn"
	"github.com/google/uuid"
)

// wauthUser adapts a Storman user to the gowa.User interface. The user handle
// (WebAuthnID) is the raw 16-byte UUID — stable across renames and small
// enough to fit the spec's 64-byte cap.
type wauthUser struct {
	id    uuid.UUID
	login string
	creds []gowa.Credential
}

func newWAUser(id uuid.UUID, login string, creds []gowa.Credential) *wauthUser {
	return &wauthUser{id: id, login: login, creds: creds}
}

func (u *wauthUser) WebAuthnID() []byte {
	b := u.id // copy; uuid.UUID is a fixed array
	return b[:]
}

func (u *wauthUser) WebAuthnName() string { return u.login }

func (u *wauthUser) WebAuthnDisplayName() string { return u.login }

func (u *wauthUser) WebAuthnCredentials() []gowa.Credential { return u.creds }
