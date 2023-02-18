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

// SignPrivateKey creates a signature from a digest using a private key and the specified algorithm.
// Note: when using EdDSA, the message gets hashed as part of the signing process, so users should normally pass the full message for the "digest" parameter.
func SignPrivateKey(digest []byte, algorithm string, key jwk.Key) (signature []byte, err error) {
	switch algorithm {
	case Algorithm_RS256, Algorithm_RS384, Algorithm_RS512:
		return signPrivateKeyRSAPKCS1v15(digest, getSHAHash(algorithm), key)

	case Algorithm_PS256, Algorithm_PS384, Algorithm_PS512:
		return signPrivateKeyRSAPSS(digest, getSHAHash(algorithm), key)

	case Algorithm_ES256, Algorithm_ES384, Algorithm_ES512:
		return signPrivateKeyECDSA(digest, key)

	case Algorithm_EdDSA:
		return signPrivateKeyEdDSA(digest, key)

	default:
		return nil, ErrUnsupportedAlgorithm
	}
}

func signPrivateKeyRSAPKCS1v15(digest []byte, hash crypto.Hash, key jwk.Key) ([]byte, error) {
	rsaKey := &rsa.PrivateKey{}
	if key.Raw(rsaKey) != nil {
		return nil, ErrKeyTypeMismatch
	}
	return rsa.SignPKCS1v15(rand.Reader, rsaKey, hash, digest)
}

func signPrivateKeyRSAPSS(digest []byte, hash crypto.Hash, key jwk.Key) ([]byte, error) {
	rsaKey := &rsa.PrivateKey{}
	if key.Raw(rsaKey) != nil {
		return nil, ErrKeyTypeMismatch
	}
	return rsa.SignPSS(rand.Reader, rsaKey, hash, digest, nil)
}

func signPrivateKeyECDSA(digest []byte, key jwk.Key) ([]byte, error) {
	ecdsaKey := &ecdsa.PrivateKey{}
	if key.Raw(ecdsaKey) != nil {
		return nil, ErrKeyTypeMismatch
	}

	r, s, err := ecdsa.Sign(rand.Reader, ecdsaKey, digest)
	if err != nil {
		return nil, err
	}
	return append(r.Bytes(), s.Bytes()...), nil
}

func signPrivateKeyEdDSA(message []byte, key jwk.Key) ([]byte, error) {
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
		return ed25519.Sign(*ed25519Key, message), nil

	default:
		return nil, ErrKeyTypeMismatch
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
		return verifyPublicKeyRSAPKCS1v15(digest, signature, getSHAHash(algorithm), key)

	case Algorithm_PS256, Algorithm_PS384, Algorithm_PS512:
		return verifyPublicKeyRSAPSS(digest, signature, getSHAHash(algorithm), key)

	case Algorithm_ES256, Algorithm_ES384, Algorithm_ES512:
		return verifyPublicKeyECDSA(digest, signature, key)

	case Algorithm_EdDSA:
		return verifyPublicKeyEdDSA(digest, signature, key)

	default:
		return false, ErrUnsupportedAlgorithm
	}
}

func verifyPublicKeyRSAPKCS1v15(digest []byte, signature []byte, hash crypto.Hash, key jwk.Key) (bool, error) {
	rsaKey := &rsa.PublicKey{}
	if key.Raw(rsaKey) != nil {
		return false, ErrKeyTypeMismatch
	}
	err := rsa.VerifyPKCS1v15(rsaKey, hash, digest, signature)
	if err != nil {
		if errors.Is(err, rsa.ErrVerification) {
			err = nil
		}
		return false, err
	}
	return true, nil
}

func verifyPublicKeyRSAPSS(digest []byte, signature []byte, hash crypto.Hash, key jwk.Key) (bool, error) {
	rsaKey := &rsa.PublicKey{}
	if key.Raw(rsaKey) != nil {
		return false, ErrKeyTypeMismatch
	}
	err := rsa.VerifyPSS(rsaKey, hash, digest, signature, nil)
	if err != nil {
		if errors.Is(err, rsa.ErrVerification) {
			err = nil
		}
		return false, err
	}
	return true, nil
}

func verifyPublicKeyECDSA(digest []byte, signature []byte, key jwk.Key) (bool, error) {
	ecdsaKey := &ecdsa.PublicKey{}
	if key.Raw(ecdsaKey) != nil {
		return false, ErrKeyTypeMismatch
	}

	n := len(signature) / 2
	r := &big.Int{}
	r.SetBytes(signature[:n])
	s := &big.Int{}
	s.SetBytes(signature[n:])

	return ecdsa.Verify(ecdsaKey, digest, r, s), nil
}

func verifyPublicKeyEdDSA(mesage []byte, signature []byte, key jwk.Key) (bool, error) {
	if key.KeyType() != jwa.OKP {
		return false, ErrKeyTypeMismatch
	}
	okpKey, ok := key.(jwk.OKPPublicKey)
	if !ok {
		return false, ErrKeyTypeMismatch
	}

	switch okpKey.Crv() {
	case jwa.Ed25519:
		ed25519Key := ed25519.PublicKey{}
		if okpKey.Raw(&ed25519Key) != nil {
			return false, ErrKeyTypeMismatch
		}
		return ed25519.Verify(ed25519Key, mesage, signature), nil

	default:
		return false, ErrKeyTypeMismatch
	}
}
