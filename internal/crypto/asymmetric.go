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

//nolint:nosnakecase
package crypto

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"math/big"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
)

// EncryptPublicKey encrypts a message using a public key and the specified algorithm.
// Note that "associatedData" is ignored if the cipher does not support labels/AAD.
func EncryptPublicKey(plaintext []byte, algorithm string, key jwk.Key, associatedData []byte) (ciphertext []byte, err error) {
	// Ensure we are using a public key
	key, err = key.PublicKey()
	if err != nil {
		return nil, ErrKeyTypeMismatch
	}

	switch algorithm {
	case Algorithm_RSA1_5:
		rsaKey := &rsa.PublicKey{}
		if key.Raw(rsaKey) != nil {
			return nil, ErrKeyTypeMismatch
		}
		return rsa.EncryptPKCS1v15(rand.Reader, rsaKey, plaintext)

	case Algorithm_RSA_OAEP:
		rsaKey := &rsa.PublicKey{}
		if key.Raw(rsaKey) != nil {
			return nil, ErrKeyTypeMismatch
		}
		return rsa.EncryptOAEP(crypto.SHA1.New(), rand.Reader, rsaKey, plaintext, associatedData)

	case Algorithm_RSA_OAEP_256, Algorithm_RSA_OAEP_384, Algorithm_RSA_OAEP_512:
		rsaKey := &rsa.PublicKey{}
		if key.Raw(rsaKey) != nil {
			return nil, ErrKeyTypeMismatch
		}
		return rsa.EncryptOAEP(getSHAHash(algorithm).New(), rand.Reader, rsaKey, plaintext, associatedData)

	default:
		return nil, ErrUnsupportedAlgorithm
	}
}

// DecryptPrivateKey decrypts a message using a private key and the specified algorithm.
// Note that "associatedData" is ignored if the cipher does not support labels/AAD.
func DecryptPrivateKey(ciphertext []byte, algorithm string, key jwk.Key, associatedData []byte) (plaintext []byte, err error) {
	switch algorithm {
	case Algorithm_RSA1_5:
		rsaKey := &rsa.PrivateKey{}
		if key.Raw(rsaKey) != nil {
			return nil, ErrKeyTypeMismatch
		}
		return rsa.DecryptPKCS1v15(rand.Reader, rsaKey, ciphertext)

	case Algorithm_RSA_OAEP:
		rsaKey := &rsa.PrivateKey{}
		if key.Raw(rsaKey) != nil {
			return nil, ErrKeyTypeMismatch
		}
		return rsa.DecryptOAEP(crypto.SHA1.New(), rand.Reader, rsaKey, ciphertext, associatedData)

	case Algorithm_RSA_OAEP_256, Algorithm_RSA_OAEP_384, Algorithm_RSA_OAEP_512:
		rsaKey := &rsa.PrivateKey{}
		if key.Raw(rsaKey) != nil {
			return nil, ErrKeyTypeMismatch
		}
		return rsa.DecryptOAEP(getSHAHash(algorithm).New(), rand.Reader, rsaKey, ciphertext, associatedData)

	default:
		return nil, ErrUnsupportedAlgorithm
	}
}

// SignPrivateKey creates a signature from a digest using a private key and the specified algorithm.
// Note: when using EdDSA, the message gets hashed as part of the signing process, so users should normally pass the full message for the "digest" parameter.
func SignPrivateKey(digest []byte, algorithm string, key jwk.Key) (signature []byte, err error) {
	switch algorithm {
	case Algorithm_RS256, Algorithm_RS384, Algorithm_RS512:
		rsaKey := &rsa.PrivateKey{}
		if key.Raw(rsaKey) != nil {
			return nil, ErrKeyTypeMismatch
		}
		return rsa.SignPKCS1v15(rand.Reader, rsaKey, getSHAHash(algorithm), digest)

	case Algorithm_PS256, Algorithm_PS384, Algorithm_PS512:
		rsaKey := &rsa.PrivateKey{}
		if key.Raw(rsaKey) != nil {
			return nil, ErrKeyTypeMismatch
		}
		return rsa.SignPSS(rand.Reader, rsaKey, getSHAHash(algorithm), digest, nil)

	case Algorithm_ES256, Algorithm_ES384, Algorithm_ES512:
		ecdsaKey := &ecdsa.PrivateKey{}
		if key.Raw(ecdsaKey) != nil {
			return nil, ErrKeyTypeMismatch
		}
		r, s, err := ecdsa.Sign(rand.Reader, ecdsaKey, digest)
		if err != nil {
			return nil, err
		}
		return append(r.Bytes(), s.Bytes()...), nil

	case Algorithm_EdDSA:
		if key.KeyType() != jwa.OKP {
			return nil, ErrKeyTypeMismatch
		}
		okpKey, ok := key.(jwk.OKPPrivateKey)
		if !ok {
			return nil, ErrKeyTypeMismatch
		}
		switch okpKey.Crv() {
		case jwa.Ed25519:
			ed25519Key := &ed25519.PrivateKey{}
			if okpKey.Raw(ed25519Key) != nil {
				return nil, ErrKeyTypeMismatch
			}

			return ed25519.Sign(*ed25519Key, digest), nil
		default:
			return nil, ErrKeyTypeMismatch
		}

	default:
		return nil, ErrUnsupportedAlgorithm
	}
}

// VerifyPublicKey validates a signature using a public key and the specified algorithm.
// Note: when using EdDSA, the message gets hashed as part of the signing process, so users should normally pass the full message for the "digest" parameter.
func VerifyPublicKey(digest []byte, signature []byte, algorithm string, key jwk.Key) (valid bool, err error) {
	// Ensure we are using a public key
	key, err = key.PublicKey()
	if err != nil {
		return false, ErrKeyTypeMismatch
	}

	switch algorithm {
	case Algorithm_RS256, Algorithm_RS384, Algorithm_RS512:
		rsaKey := &rsa.PublicKey{}
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

	case Algorithm_PS256, Algorithm_PS384, Algorithm_PS512:
		rsaKey := &rsa.PublicKey{}
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

	case Algorithm_ES256, Algorithm_ES384, Algorithm_ES512:
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

	case Algorithm_EdDSA:
		if key.KeyType() != jwa.OKP {
			return false, ErrKeyTypeMismatch
		}
		okpKey, ok := key.(jwk.OKPPublicKey)
		if !ok {
			return false, ErrKeyTypeMismatch
		}
		switch okpKey.Crv() {
		case jwa.Ed25519:
			var ed25519Key ed25519.PublicKey
			if okpKey.Raw(&ed25519Key) != nil {
				return false, ErrKeyTypeMismatch
			}

			return ed25519.Verify(ed25519Key, digest, signature), nil
		default:
			return false, ErrKeyTypeMismatch
		}

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
