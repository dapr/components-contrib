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
	"errors"
)

// Errors
var (
	// ErrUnsupportedAlgorithm is returned when the algorithm is unsupported.
	ErrUnsupportedAlgorithm = errors.New("unsupported algorithm")
	// ErrKeyTypeMismatch is returned when the key type doesn't match the requested algorithm.
	ErrKeyTypeMismatch = errors.New("key type mismatch")
	// ErrInvalidNonce is returned when the nonce isn't valid for the requested operation.
	ErrInvalidNonce = errors.New("invalid nonce")
)

// Algorithms
const (
	Algorithm_A128CBC        = "A128CBC"        // Encryption: AES-CBC, 128-bit key
	Algorithm_A192CBC        = "A192CBC"        // Encryption: AES-CBC, 192-bit key
	Algorithm_A256CBC        = "A256CBC"        // Encryption: AES-CBC, 256-bit key
	Algorithm_A128GCM        = "A128GCM"        // Encryption: AES-GCM, 128-bit key
	Algorithm_A192GCM        = "A192GCM"        // Encryption: AES-GCM, 192-bit key
	Algorithm_A256GCM        = "A256GCM"        // Encryption: AES-GCM, 256-bit key
	Algorithm_A128CBC_HS256  = "A128CBC-HS256"  // Encryption: AES-CBC + HMAC-SHA256, 128-bit key
	Algorithm_A192CBC_HS384  = "A192CBC-HS384"  // Encryption: AES-CBC + HMAC-SHA384, 192-bit key
	Algorithm_A256CBC_HS512  = "A256CBC-HS512"  // Encryption: AES-CBC + HMAC-SHA512, 256-bit key
	Algorithm_A128KW         = "A128KW"         // Encryption: AES Key Wrap (RFC 3394), 128-bit key
	Algorithm_A192KW         = "A192KW"         // Encryption: AES Key Wrap (RFC 3394), 192-bit key
	Algorithm_A256KW         = "A256KW"         // Encryption: AES Key Wrap (RFC 3394), 256-bit key
	Algorithm_A128GCMKW      = "A128GCMKW"      // Encryption: AES-GCM key wrap, 128-bit key
	Algorithm_A192GCMKW      = "A192GCMKW"      // Encryption: AES-GCM key wrap, 192-bit key
	Algorithm_A256GCMKW      = "A256GCMKW"      // Encryption: AES-GCM key wrap, 256-bit key
	Algorithm_C20P           = "C20P"           // Encryption: ChaCha20-Poly1305, 96-bit IV
	Algorithm_XC20P          = "XC20P"          // Encryption: XChaCha20-Poly1305, 192-bit IV
	Algorithm_C20PKW         = "C20PKW"         // Encryption: ChaCha20-Poly1305 key wrap, 96-bit IV
	Algorithm_XC20PKW        = "XC20PKW"        // Encryption: XChaCha20-Poly1305 key wrap, 192-bit IV
	Algorithm_ECDH_ES        = "ECDH-ES"        // Encryption: ECDH-ES
	Algorithm_ECDH_ES_A128KW = "ECDH-ES+A128KW" // Encryption: ECDH-ES + AES key wrap, 128-bit key
	Algorithm_ECDH_ES_A192KW = "ECDH-ES+A192KW" // Encryption: ECDH-ES + AES key wrap, 192-bit key
	Algorithm_ECDH_ES_A256KW = "ECDH-ES+A256KW" // Encryption: ECDH-ES + AES key wrap, 256-bit key
	Algorithm_RSA1_5         = "RSA1_5"         // Encryption: RSA-PKCS1v1.5
	Algorithm_RSA_OAEP       = "RSA-OAEP"       // Encryption: RSA-OAEP with SHA1 hash
	Algorithm_RSA_OAEP_256   = "RSA-OAEP-256"   // Encryption: RSA-OAEP with SHA256 hash
	Algorithm_RSA_OAEP_384   = "RSA-OAEP-384"   // Encryption: RSA-OAEP with SHA384 hash
	Algorithm_RSA_OAEP_512   = "RSA-OAEP-512"   // Encryption: RSA-OAEP with SHA512 hash
	Algorithm_ES256          = "ES256"          // Signature: ECDSA using P-256 and SHA-256
	Algorithm_ES384          = "ES384"          // Signature: ECDSA using P-384 and SHA-384
	Algorithm_ES512          = "ES512"          // Signature: ECDSA using P-521 and SHA-512
	Algorithm_EdDSA          = "EdDSA"          // Signature: EdDSA signature algorithms
	Algorithm_HS256          = "HS256"          // Signature: HMAC using SHA-256
	Algorithm_HS384          = "HS384"          // Signature: HMAC using SHA-384
	Algorithm_HS512          = "HS512"          // Signature: HMAC using SHA-512
	Algorithm_PS256          = "PS256"          // Signature: RSASSA-PSS using SHA256 and MGF1-SHA256
	Algorithm_PS384          = "PS384"          // Signature: RSASSA-PSS using SHA384 and MGF1-SHA384
	Algorithm_PS512          = "PS512"          // Signature: RSASSA-PSS using SHA512 and MGF1-SHA512
	Algorithm_RS256          = "RS256"          // Signature: RSASSA-PKCS-v1.5 using SHA-256
	Algorithm_RS384          = "RS384"          // Signature: RSASSA-PKCS-v1.5 using SHA-384
	Algorithm_RS512          = "RS512"          // Signature: RSASSA-PKCS-v1.5 using SHA-512
)
