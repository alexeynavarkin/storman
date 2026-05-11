package auth

import (
	"errors"
	"testing"
)

func TestHashPasswordRoundtrip(t *testing.T) {
	params := HashParams{MemoryKiB: 8 * 1024, Iterations: 1, Parallelism: 1, SaltLen: 16, KeyLen: 32}
	h, err := HashPassword("correct horse battery staple", params)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyPassword("correct horse battery staple", h); err != nil {
		t.Errorf("verify matching: %v", err)
	}
	if err := VerifyPassword("wrong", h); !errors.Is(err, ErrPasswordMismatch) {
		t.Errorf("expected ErrPasswordMismatch, got %v", err)
	}
}

func TestVerifyRejectsMalformed(t *testing.T) {
	if err := VerifyPassword("x", "not-a-hash"); err == nil {
		t.Fatal("expected error on malformed hash")
	}
	if err := VerifyPassword("x", "$argon2i$v=19$m=1,t=1,p=1$YWFh$YmJi"); err == nil {
		t.Fatal("expected error on non-argon2id variant")
	}
}
