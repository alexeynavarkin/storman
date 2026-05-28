package webauthn

import "encoding/base64"

// decodeB64URL decodes a base64url string, accepting both padded and unpadded
// forms. gowa.SessionData.Challenge is unpadded base64url, but defensively
// handling the padded variant costs nothing.
func decodeB64URL(s string) ([]byte, error) {
	if b, err := base64.RawURLEncoding.DecodeString(s); err == nil {
		return b, nil
	}
	return base64.URLEncoding.DecodeString(s)
}

// encodeB64URL is the inverse — used by the HTTP layer to ship challenge IDs
// to the browser.
func encodeB64URL(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}
