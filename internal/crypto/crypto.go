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
)

// Encrypt a message using the given algorithm and key, supporting both symmetric and asymmetric ciphers.
func Encrypt(plaintext []byte, algorithm string, key jwk.Key, nonce []byte, associatedData []byte) (ciphertext []byte, tag []byte, err error) {
	// Note that this includes all constants defined in consts.go, but some algorithms are not supported (yet)
	switch algorithm {
	case Algorithm_A128CBC, Algorithm_A192CBC, Algorithm_A256CBC,
		Algorithm_A128GCM, Algorithm_A192GCM, Algorithm_A256GCM,
		Algorithm_A128CBC_HS256, Algorithm_A192CBC_HS384, Algorithm_A256CBC_HS512,
		Algorithm_A128KW, Algorithm_A192KW, Algorithm_A256KW,
		Algorithm_A128GCMKW, Algorithm_A192GCMKW, Algorithm_A256GCMKW,
		Algorithm_C20P, Algorithm_XC20P, Algorithm_C20PKW, Algorithm_XC20PKW:
		return EncryptSymmetric(plaintext, algorithm, key, nonce, associatedData)

	case Algorithm_ECDH_ES,
		Algorithm_ECDH_ES_A128KW, Algorithm_ECDH_ES_A192KW, Algorithm_ECDH_ES_A256KW,
		Algorithm_RSA1_5,
		Algorithm_RSA_OAEP, Algorithm_RSA_OAEP_256, Algorithm_RSA_OAEP_384, Algorithm_RSA_OAEP_512:
		ciphertext, err = EncryptPublicKey(plaintext, algorithm, key, associatedData)
		return

	default:
		return nil, nil, ErrUnsupportedAlgorithm
	}
}

// Decrypt a message using the given algorithm and key, supporting both symmetric and asymmetric ciphers.
func Decrypt(ciphertext []byte, algorithm string, key jwk.Key, nonce []byte, tag []byte, associatedData []byte) (plaintext []byte, err error) {
	// Note that this includes all constants defined in consts.go, but some algorithms are not supported (yet)
	switch algorithm {
	case Algorithm_A128CBC, Algorithm_A192CBC, Algorithm_A256CBC,
		Algorithm_A128GCM, Algorithm_A192GCM, Algorithm_A256GCM,
		Algorithm_A128CBC_HS256, Algorithm_A192CBC_HS384, Algorithm_A256CBC_HS512,
		Algorithm_A128KW, Algorithm_A192KW, Algorithm_A256KW,
		Algorithm_A128GCMKW, Algorithm_A192GCMKW, Algorithm_A256GCMKW,
		Algorithm_C20P, Algorithm_XC20P, Algorithm_C20PKW, Algorithm_XC20PKW:
		return DecryptSymmetric(ciphertext, algorithm, key, nonce, tag, associatedData)

	case Algorithm_ECDH_ES,
		Algorithm_ECDH_ES_A128KW, Algorithm_ECDH_ES_A192KW, Algorithm_ECDH_ES_A256KW,
		Algorithm_RSA1_5,
		Algorithm_RSA_OAEP, Algorithm_RSA_OAEP_256, Algorithm_RSA_OAEP_384, Algorithm_RSA_OAEP_512:
		return DecryptPrivateKey(ciphertext, algorithm, key, associatedData)

	default:
		return nil, ErrUnsupportedAlgorithm
	}
}
