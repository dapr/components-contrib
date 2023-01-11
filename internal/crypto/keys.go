/*
Copyright 2022 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package crypto

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/lestrrat-go/jwx/v2/jwk"
)

// SerializeKey serializes a jwk.Key in the appropriate format so they can be wrapped.
// Symmetric keys are returned as raw bytes, while asymmetric keys are marshalled as ASN.1 DER (X.509, not PEM-encoded).
func SerializeKey(key jwk.Key) ([]byte, error) {
	var rawKey any
	err := key.Raw(&rawKey)
	if err != nil {
		return nil, fmt.Errorf("failed to extract raw key: %w", err)
	}

	switch r := rawKey.(type) {
	case []byte: // Symmetric keys
		return r, nil
	case *rsa.PrivateKey, *ecdsa.PrivateKey, ed25519.PrivateKey: // Private keys: marshal as PKCS#8
		return x509.MarshalPKCS8PrivateKey(r)
	case *rsa.PublicKey, *ecdsa.PublicKey, ed25519.PublicKey: // Public keys: marshal as PKIX
		return x509.MarshalPKIXPublicKey(r)
	default:
		return nil, errors.New("unsupported key type")
	}
}

// ParseKey takes a byte slice and returns a key (public, private, symmetric) after determining its type.
// It supports keys represented as JWKs, PEM-encoded (PKCS#8, PKCS#1 or PKIX) or as raw bytes (optionally base64-encoded).
// The parameter contentType is optional and it can contain a mime type.
func ParseKey(raw []byte, contentType string) (jwk.Key, error) {
	// Determine the type of key if the type parameter is set
	switch contentType {
	case "application/json": // JWK
		return jwk.ParseKey(raw)
	case "application/x-pem-file", "application/pkcs8": // Generic PEM: PKCS#1, PKCS#8, PKIX
		return jwk.ParseKey(raw, jwk.WithPEM(true))
	}

	// Heuristically determine the type of key
	switch {
	case raw[0] == '{': // Assume it's a JWK
		return jwk.ParseKey(raw)
	case len(raw) > 10 && string(raw[0:5]) == ("-----"): // Assume it's something PEM-encoded
		return jwk.ParseKey(raw, jwk.WithPEM(true))
	default: // Assume a symmetric key
		return parseSymmetricKey(raw)
	}
}

func parseSymmetricKey(raw []byte) (jwk.Key, error) {
	l := len(raw)

	// Fast path: if the slice is 16, 24, or 32 bytes, assume it's the actual key
	if l == 16 || l == 24 || l == 32 {
		return jwk.FromRaw(raw)
	}

	// Try parsing as base64; first: remove any padding if present
	trimmedRaw := bytes.TrimRight(raw, "=")

	// Try parsing as base64-standard
	dst := make([]byte, base64.RawStdEncoding.DecodedLen(l))
	n, err := base64.RawStdEncoding.Decode(dst, trimmedRaw)
	if err == nil {
		return jwk.FromRaw(dst[:n])
	}

	// Try parsing as base64-url
	n, err = base64.RawURLEncoding.Decode(dst, trimmedRaw)
	if err == nil {
		return jwk.FromRaw(dst[:n])
	}

	// Treat the byte slice as the raw, symmetric key
	return jwk.FromRaw(raw)
}
