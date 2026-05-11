// Package auth owns user identity: password hashing, session lifecycle,
// share-links. It is a service layer with no HTTP/FTP wiring of its own.
package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

// HashParams configures argon2id. Defaults match docs/arch/auth.md.
type HashParams struct {
	MemoryKiB   uint32 // memory cost in KiB
	Iterations  uint32 // time cost
	Parallelism uint8
	SaltLen     uint32
	KeyLen      uint32
}

// DefaultHashParams: 64 MiB memory, 3 iterations, 2 parallel lanes.
func DefaultHashParams() HashParams {
	return HashParams{
		MemoryKiB:   64 * 1024,
		Iterations:  3,
		Parallelism: 2,
		SaltLen:     16,
		KeyLen:      32,
	}
}

// ErrPasswordMismatch indicates a non-matching password during verification.
var ErrPasswordMismatch = errors.New("auth: password mismatch")

// HashPassword returns the standard argon2id encoded form:
//
//	$argon2id$v=19$m=<mem>,t=<iter>,p=<par>$<salt-b64>$<hash-b64>
//
// The encoded form carries parameters with the hash so rotating defaults
// does not invalidate existing credentials.
func HashPassword(password string, p HashParams) (string, error) {
	salt := make([]byte, p.SaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("salt: %w", err)
	}
	key := argon2.IDKey([]byte(password), salt, p.Iterations, p.MemoryKiB, p.Parallelism, p.KeyLen)
	return fmt.Sprintf(
		"$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, p.MemoryKiB, p.Iterations, p.Parallelism,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(key),
	), nil
}

// VerifyPassword recomputes the hash with the embedded parameters and compares
// in constant time. Returns ErrPasswordMismatch on a clean mismatch and a
// distinct error if the encoded form is malformed.
func VerifyPassword(password, encoded string) error {
	parts := strings.Split(encoded, "$")
	// Expected: ["", "argon2id", "v=19", "m=...,t=...,p=...", "<salt>", "<hash>"]
	if len(parts) != 6 || parts[1] != "argon2id" {
		return errors.New("auth: unsupported password hash format")
	}
	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil {
		return fmt.Errorf("auth: version parse: %w", err)
	}
	if version != argon2.Version {
		return fmt.Errorf("auth: unsupported argon2 version %d", version)
	}
	var mem, iter uint32
	var par uint8
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &mem, &iter, &par); err != nil {
		return fmt.Errorf("auth: params parse: %w", err)
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return fmt.Errorf("auth: salt decode: %w", err)
	}
	want, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return fmt.Errorf("auth: hash decode: %w", err)
	}
	got := argon2.IDKey([]byte(password), salt, iter, mem, par, uint32(len(want)))
	if subtle.ConstantTimeCompare(want, got) != 1 {
		return ErrPasswordMismatch
	}
	return nil
}
