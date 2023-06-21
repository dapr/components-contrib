/*
Copyright 2023 The Dapr Authors
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
	"encoding/json"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwk"
	"golang.org/x/exp/slices"
)

// Key extends jwk.Key adding optional properties for determining if the key is valid (time bounds) or can be used for certain purposes.
type Key struct {
	jwk.Key

	kid string
	exp *time.Time
	nbf *time.Time
}

// NewKey returns a new Key object
func NewKey(key jwk.Key, kid string, exp, nbf *time.Time) *Key {
	return &Key{
		Key: key,
		kid: kid,
		exp: exp,
		nbf: nbf,
	}
}

// KeyID returns the value of the kid (key ID) property if present.
func (k Key) KeyID() string {
	return k.kid
}

// CanPerformOperation returns true if the key can be used to perform a specific operation.
func (k Key) CanPerformOperation(op jwk.KeyOperation) bool {
	return KeyCanPerformOperation(k, op)
}

// IsValid checks if the key is within the time bounds of validity.
func (k Key) IsValid() bool {
	return k.isValidAtTime(time.Now())
}

func (k Key) isValidAtTime(t time.Time) bool {
	if k.exp != nil && k.exp.Before(t) {
		return false
	}

	if k.nbf != nil && k.nbf.After(t) {
		return false
	}

	return true
}

// MarshalJSON implements the json.Marshaler interface
func (k Key) MarshalJSON() ([]byte, error) {
	// Marshal the Key property only
	return json.Marshal(k.Key)
}

// KeyCanPerformOperation returns true if the key can be used to perform a specific operation.
func KeyCanPerformOperation(key jwk.Key, op jwk.KeyOperation) bool {
	// keyUsage is the value of "use" ("sig" or "enc"), while keyOps is the value of "key_ops" (an array of allowed operations)
	// Per RFC 7517: `The "use" and "key_ops" JWK members SHOULD NOT be used together; however, if both are used, the information they convey MUST be consistent.`
	keyUsage := key.KeyUsage()
	keyOps := key.KeyOps()

	// If the key has nothin in both fields, then just allow any operation
	if len(keyOps) == 0 && keyUsage == "" {
		return true
	}

	// Check key_ops
	if len(keyOps) > 0 {
		if !slices.Contains(keyOps, op) {
			return false
		}
	}

	// Check use
	if keyUsage != "" {
		switch op {
		case jwk.KeyOpEncrypt, jwk.KeyOpDecrypt, jwk.KeyOpWrapKey, jwk.KeyOpUnwrapKey:
			return keyUsage == "enc"
		case jwk.KeyOpSign, jwk.KeyOpVerify:
			return keyUsage == "sig"
		}
	}

	return true
}

// KeyCanPerformAlgorithm returns true if the key can be used with a specific algorithm.
func KeyCanPerformAlgorithm(key jwk.Key, alg string) bool {
	// "alg" is the supported algorithm
	var keyAlg string
	if key != nil {
		keyAlg = key.Algorithm().String()
	}
	// If there's no "alg", then allow the operation
	if keyAlg == "" {
		return true
	}

	return alg == keyAlg
}
