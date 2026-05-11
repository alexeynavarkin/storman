package web

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
)

const (
	csrfCookieName = "storman_csrf"
	csrfHeaderName = "X-CSRF-Token"
	csrfTokenLen   = 32
)

// newCSRFToken returns a fresh random token, base64url-encoded.
func newCSRFToken() (string, error) {
	var b [csrfTokenLen]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

// csrfMatches compares the cookie value with the header value in constant time.
// Empty strings never match.
func csrfMatches(cookieVal, headerVal string) bool {
	if cookieVal == "" || headerVal == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(cookieVal), []byte(headerVal)) == 1
}
