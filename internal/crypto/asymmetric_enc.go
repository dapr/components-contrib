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
	"crypto/rand"
	"crypto/rsa"

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
		return encryptPublicKeyRSAPKCS1v15(plaintext, key)

	case Algorithm_RSA_OAEP:
		return encryptPublicKeyRSAOAEP(plaintext, key, crypto.SHA1, associatedData)

	case Algorithm_RSA_OAEP_256, Algorithm_RSA_OAEP_384, Algorithm_RSA_OAEP_512:
		return encryptPublicKeyRSAOAEP(plaintext, key, getSHAHash(algorithm), associatedData)

	default:
		return nil, ErrUnsupportedAlgorithm
	}
}

func encryptPublicKeyRSAPKCS1v15(plaintext []byte, key jwk.Key) ([]byte, error) {
	rsaKey := &rsa.PublicKey{}
	if key.Raw(rsaKey) != nil {
		return nil, ErrKeyTypeMismatch
	}
	return rsa.EncryptPKCS1v15(rand.Reader, rsaKey, plaintext)
}

func encryptPublicKeyRSAOAEP(plaintext []byte, key jwk.Key, hash crypto.Hash, label []byte) ([]byte, error) {
	rsaKey := &rsa.PublicKey{}
	if key.Raw(rsaKey) != nil {
		return nil, ErrKeyTypeMismatch
	}
	return rsa.EncryptOAEP(hash.New(), rand.Reader, rsaKey, plaintext, label)
}

// DecryptPrivateKey decrypts a message using a private key and the specified algorithm.
// Note that "associatedData" is ignored if the cipher does not support labels/AAD.
func DecryptPrivateKey(ciphertext []byte, algorithm string, key jwk.Key, associatedData []byte) (plaintext []byte, err error) {
	switch algorithm {
	case Algorithm_RSA1_5:
		return decryptPrivateKeyRSAPKCS1v15(ciphertext, key)

	case Algorithm_RSA_OAEP:
		return decryptPrivateKeyRSAOAEP(ciphertext, key, crypto.SHA1, associatedData)

	case Algorithm_RSA_OAEP_256, Algorithm_RSA_OAEP_384, Algorithm_RSA_OAEP_512:
		return decryptPrivateKeyRSAOAEP(ciphertext, key, getSHAHash(algorithm), associatedData)

	default:
		return nil, ErrUnsupportedAlgorithm
	}
}

func decryptPrivateKeyRSAPKCS1v15(ciphertext []byte, key jwk.Key) ([]byte, error) {
	rsaKey := &rsa.PrivateKey{}
	if key.Raw(rsaKey) != nil {
		return nil, ErrKeyTypeMismatch
	}
	return rsa.DecryptPKCS1v15(rand.Reader, rsaKey, ciphertext)
}

func decryptPrivateKeyRSAOAEP(ciphertext []byte, key jwk.Key, hash crypto.Hash, label []byte) ([]byte, error) {
	rsaKey := &rsa.PrivateKey{}
	if key.Raw(rsaKey) != nil {
		return nil, ErrKeyTypeMismatch
	}
	return rsa.DecryptOAEP(hash.New(), rand.Reader, rsaKey, ciphertext, label)
}
