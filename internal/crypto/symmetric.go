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
	"crypto/aes"
	"crypto/cipher"
	"errors"

	"golang.org/x/crypto/chacha20poly1305"

	"github.com/dapr/components-contrib/internal/crypto/aeskw"
)

// EncryptSymmetric encrypts a message using a symmetric key and the specified algorithm.
// Note that "associatedData" is ignored if the cipher does not support labels/AAD.
func EncryptSymmetric(plaintext []byte, algorithm string, key []byte, nonce []byte, associatedData []byte) (ciphertext []byte, tag []byte, err error) {
	switch algorithm {
	case Algorithm_A128CBC, Algorithm_A192CBC, Algorithm_A256CBC:
		ciphertext, err = encryptSymmetricAESCBC(plaintext, algorithm, key, nonce)
		return ciphertext, tag, err

	case Algorithm_A128GCM, Algorithm_A192GCM, Algorithm_A256GCM:
		return encryptSymmetricAESGCM(plaintext, algorithm, key, nonce, associatedData)

	case Algorithm_A128KW, Algorithm_A192KW, Algorithm_A256KW:
		ciphertext, err = encryptSymmetricAESKW(plaintext, algorithm, key)
		return ciphertext, tag, err

	case Algorithm_C20P, Algorithm_C20PKW, Algorithm_XC20P, Algorithm_XC20PKW:
		return encryptSymmetricChaCha20Poly1305(plaintext, algorithm, key, nonce, associatedData)

	default:
		return nil, nil, ErrUnsupportedAlgorithm
	}
}

// DecryptSymmetric decrypts an encrypted message using a symmetric key and the specified algorithm.
// Note that "associatedData" is ignored if the cipher does not support labels/AAD.
func DecryptSymmetric(ciphertext []byte, algorithm string, key []byte, nonce []byte, tag []byte, associatedData []byte) (plaintext []byte, err error) {
	switch algorithm {
	case Algorithm_A128CBC, Algorithm_A192CBC, Algorithm_A256CBC:
		return decryptSymmetricAESCBC(ciphertext, algorithm, key, nonce)

	case Algorithm_A128GCM, Algorithm_A192GCM, Algorithm_A256GCM:
		return decryptSymmetricAESGCM(ciphertext, algorithm, key, nonce, tag, associatedData)

	case Algorithm_A128KW, Algorithm_A192KW, Algorithm_A256KW:
		return decryptSymmetricAESKW(ciphertext, algorithm, key)

	case Algorithm_C20P, Algorithm_C20PKW, Algorithm_XC20P, Algorithm_XC20PKW:
		return decryptSymmetricChaCha20Poly1305(ciphertext, algorithm, key, nonce, tag, associatedData)

	default:
		return nil, ErrUnsupportedAlgorithm
	}
}

func encryptSymmetricAESCBC(plaintext []byte, algorithm string, key []byte, iv []byte) (ciphertext []byte, err error) {
	if len(key) != expectedKeySize(algorithm) {
		return nil, ErrKeyTypeMismatch
	}
	if len(iv) != aes.BlockSize {
		return nil, ErrInvalidNonce
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrKeyTypeMismatch
	}

	ciphertext = make([]byte, len(plaintext))
	cipher.NewCBCEncrypter(block, iv).
		CryptBlocks(ciphertext, plaintext)

	return ciphertext, err
}

func decryptSymmetricAESCBC(ciphertext []byte, algorithm string, key []byte, iv []byte) (plaintext []byte, err error) {
	if len(key) != expectedKeySize(algorithm) {
		return nil, ErrKeyTypeMismatch
	}
	if len(iv) != aes.BlockSize {
		return nil, ErrInvalidNonce
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrKeyTypeMismatch
	}

	plaintext = make([]byte, len(ciphertext))
	cipher.NewCBCDecrypter(block, iv).
		CryptBlocks(plaintext, ciphertext)

	return plaintext, err
}

func encryptSymmetricAESGCM(plaintext []byte, algorithm string, key []byte, nonce []byte, associatedData []byte) (ciphertext []byte, tag []byte, err error) {
	if len(key) != expectedKeySize(algorithm) {
		return nil, nil, ErrKeyTypeMismatch
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, nil, ErrKeyTypeMismatch
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, ErrKeyTypeMismatch
	}

	if len(nonce) != aead.NonceSize() {
		return nil, nil, ErrInvalidNonce
	}

	out := aead.Seal(nil, nonce, plaintext, associatedData)
	// Tag is added at the end
	tagSize := aead.Overhead()
	return out[0 : len(out)-tagSize], out[len(out)-tagSize:], nil
}

func decryptSymmetricAESGCM(ciphertext []byte, algorithm string, key []byte, nonce []byte, tag []byte, associatedData []byte) (plaintext []byte, err error) {
	if len(key) != expectedKeySize(algorithm) {
		return nil, ErrKeyTypeMismatch
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrKeyTypeMismatch
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, ErrKeyTypeMismatch
	}

	if len(nonce) != aead.NonceSize() {
		return nil, ErrInvalidNonce
	}

	if len(tag) != aead.Overhead() {
		return nil, ErrInvalidTag
	}

	// Add the tag at the end of the ciphertext
	ciphertext = append(ciphertext, tag...)
	return aead.Open(nil, nonce, ciphertext, associatedData)
}

func encryptSymmetricAESKW(plaintext []byte, algorithm string, key []byte) (ciphertext []byte, err error) {
	if len(key) != expectedKeySize(algorithm) {
		return nil, ErrKeyTypeMismatch
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrKeyTypeMismatch
	}

	return aeskw.Wrap(block, plaintext)
}

func decryptSymmetricAESKW(ciphertext []byte, algorithm string, key []byte) (plaintext []byte, err error) {
	if len(key) != expectedKeySize(algorithm) {
		return nil, ErrKeyTypeMismatch
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrKeyTypeMismatch
	}

	return aeskw.Unwrap(block, ciphertext)
}

func encryptSymmetricChaCha20Poly1305(plaintext []byte, algorithm string, key []byte, nonce []byte, associatedData []byte) (ciphertext []byte, tag []byte, err error) {
	if len(key) != chacha20poly1305.KeySize {
		return nil, nil, ErrKeyTypeMismatch
	}

	aead, err := getChaCha20Poly1305Cipher(algorithm, key, nonce)
	if err != nil {
		return nil, nil, err
	}

	// Tag is added at the end
	out := aead.Seal(nil, nonce, plaintext, associatedData)
	return out[0 : len(out)-chacha20poly1305.Overhead], out[len(out)-chacha20poly1305.Overhead:], nil
}

func decryptSymmetricChaCha20Poly1305(ciphertext []byte, algorithm string, key []byte, nonce []byte, tag []byte, associatedData []byte) (plaintext []byte, err error) {
	if len(key) != chacha20poly1305.KeySize {
		return nil, ErrKeyTypeMismatch
	}

	aead, err := getChaCha20Poly1305Cipher(algorithm, key, nonce)
	if err != nil {
		return nil, err
	}

	if len(tag) != aead.Overhead() {
		return nil, ErrInvalidTag
	}

	// Add the tag at the end of the ciphertext
	ciphertext = append(ciphertext, tag...)
	return aead.Open(nil, nonce, ciphertext, associatedData)
}

func getChaCha20Poly1305Cipher(algorithm string, key []byte, nonce []byte) (aead cipher.AEAD, err error) {
	switch algorithm {
	case Algorithm_C20P, Algorithm_C20PKW:
		aead, err = chacha20poly1305.New(key)
		if err == nil && len(nonce) != chacha20poly1305.NonceSize {
			err = ErrInvalidNonce
		}
		return

	case Algorithm_XC20P, Algorithm_XC20PKW:
		aead, err = chacha20poly1305.NewX(key)
		if err == nil && len(nonce) != chacha20poly1305.NonceSizeX {
			err = ErrInvalidNonce
		}
		return
	}

	return nil, errors.New("invalid algorithm")
}

func expectedKeySize(alg string) int {
	switch alg[1:4] {
	case "128":
		return 16
	case "192":
		return 24
	case "256":
		return 32
	}
	return 0
}
