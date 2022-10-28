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
	"github.com/lestrrat-go/jwx/v2/jwk"

	internals "github.com/dapr/components-contrib/internal/crypto"
)

// SerializeKey serializes a jwk.Key in the appropriate format so they can be wrapped.
// Symmetric keys are returned as raw bytes, while asymmetric keys are marshalled as ASN.1 DER (X.509, not PEM-encoded).
//
// TODO: TEMPORARY UNTIL internal/crypto IS MOVED TO dapr/kit.
func SerializeKey(key jwk.Key) ([]byte, error) {
	return internals.SerializeKey(key)
}

// ParseKey takes a byte slice and returns a key (public, private, symmetric) after determining its type.
// It supports keys represented as JWKs, PEM-encoded (PKCS#8, PKCS#1 or PKIX) or as raw bytes (optionally base64-encoded).
// The parameter contentType is optional and it can contain a mime type.
//
// TODO: TEMPORARY UNTIL internal/crypto IS MOVED TO dapr/kit.
func ParseKey(raw []byte, contentType string) (jwk.Key, error) {
	return internals.ParseKey(raw, contentType)
}
