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
	"crypto"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"math/big"

	"github.com/lestrrat-go/jwx/v2/jwk"

	daprcrypto "github.com/dapr/components-contrib/crypto"
)

// Errors
var (
	// ErrUnsupportedAlgorithm is returned when the algorithm is unsupported.
	ErrUnsupportedAlgorithm = errors.New("unsupported algorithm")
	// ErrKeyTypeMismatch is returned when the key type doesn't match the requested algorithm.
	ErrKeyTypeMismatch = errors.New("key type mismatch")
)

// EncryptPublicKey encrypts a message using a public key and the specified algorithm.
// Note that "associatedData" is ignored if the cipher does not support labels/AAD.
func EncryptPublicKey(plaintext []byte, algorithm string, key *daprcrypto.Key, associatedData []byte) (ciphertext []byte, err error) {
	switch algorithm {
	case "RSA1_5":
		var rsaKey *rsa.PublicKey
		if key.Raw(rsaKey) != nil {
			return nil, ErrKeyTypeMismatch
		}
		return rsa.EncryptPKCS1v15(rand.Reader, rsaKey, plaintext)

	case "RSA-OAEP":
		var rsaKey *rsa.PublicKey
		if key.Raw(rsaKey) != nil {
			return nil, ErrKeyTypeMismatch
		}
		return rsa.EncryptOAEP(crypto.SHA1.New(), rand.Reader, rsaKey, plaintext, associatedData)

	case "RSA-OAEP-256", "RSA-OAEP-384", "RSA-OAEP-512":
		var rsaKey *rsa.PublicKey
		if key.Raw(rsaKey) != nil {
			return nil, ErrKeyTypeMismatch
		}
		return rsa.EncryptOAEP(getSHAHash(algorithm).New(), rand.Reader, rsaKey, plaintext, associatedData)

	default:
		return nil, ErrUnsupportedAlgorithm
	}
}

// EncryptPublicKey validates a signature using a public key and the specified algorithm.
func VerifyPublicKey(digest []byte, signature []byte, algorithm string, key jwk.Key) (valid bool, err error) {
	switch algorithm {
	case "RS256", "RS384", "RS512":
		var rsaKey *rsa.PublicKey
		if key.Raw(rsaKey) != nil {
			return false, ErrKeyTypeMismatch
		}
		err = rsa.VerifyPKCS1v15(rsaKey, getSHAHash(algorithm), digest, signature)
		if err != nil {
			if errors.Is(err, rsa.ErrVerification) {
				err = nil
			}
			return false, err
		}
		return true, nil

	case "PS256", "PS384", "PS512":
		var rsaKey *rsa.PublicKey
		if key.Raw(rsaKey) != nil {
			return false, ErrKeyTypeMismatch
		}
		err = rsa.VerifyPSS(rsaKey, getSHAHash(algorithm), digest, signature, nil)
		if err != nil {
			if errors.Is(err, rsa.ErrVerification) {
				err = nil
			}
			return false, err
		}
		return true, nil

	case "ES256", "ES384", "ES512":
		var ecdsaKey *ecdsa.PublicKey
		if key.Raw(ecdsaKey) != nil {
			return false, ErrKeyTypeMismatch
		}

		n := len(signature) / 2
		r := &big.Int{}
		r.SetBytes(signature[:n])
		s := &big.Int{}
		s.SetBytes(signature[n:])

		return ecdsa.Verify(ecdsaKey, digest, r, s), nil
	default:
		return false, ErrUnsupportedAlgorithm
	}
}

func getSHAHash(alg string) crypto.Hash {
	switch alg[len(alg)-3:] {
	case "256":
		return crypto.SHA256
	case "384":
		return crypto.SHA384
	case "512":
		return crypto.SHA512
	}
	return crypto.Hash(0)
}
