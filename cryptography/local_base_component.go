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
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"

	internals "github.com/dapr/kit/crypto"
)

// ErrKeyNotFound is returned when the key could not be found.
var ErrKeyNotFound = errors.New("key not found")

var (
	// Token used to populate the list of supported algorithms.
	supportedAlgsOnce             sync.Once
	supportedEncryptionAlgorithms []string
	supportedSignatureAlgorithms  []string
)

// LocalCryptoBaseComponent is an "abstract" component that performs cryptographic operations locally in the Dapr runtime.
// Concrete implementations build on top of this component and just need to provide retrieveKeyFromSecret.
// Examples of components that build on top of this: crypto.kubernetes.secrets, crypto.jwks
type LocalCryptoBaseComponent struct {
	// RetrieveKeyFn is the function used to retrieve a key, and must be passed by concrete implementations
	RetrieveKeyFn func(parentCtx context.Context, key string) (jwk.Key, error)
}

func (k LocalCryptoBaseComponent) GetKey(parentCtx context.Context, key string) (pubKey jwk.Key, err error) {
	jwkObj, err := k.RetrieveKeyFn(parentCtx, key)
	if err != nil {
		return nil, err
	}

	// We can only return public keys
	if jwkObj.KeyType() == jwa.OctetSeq {
		// If the key exists but it's not of the right kind, return a not-found error
		return nil, ErrKeyNotFound
	}
	pk, err := jwkObj.PublicKey()
	if err != nil {
		return nil, fmt.Errorf("failed to obtain public key: %w", err)
	}
	return pk, nil
}

func (k LocalCryptoBaseComponent) Encrypt(parentCtx context.Context, plaintext []byte, algorithm string, keyName string, nonce []byte, associatedData []byte) (ciphertext []byte, tag []byte, err error) {
	// Retrieve the key
	key, err := k.RetrieveKeyFn(parentCtx, keyName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to retrieve the key: %w", err)
	}

	// Check if the key can perform the operation
	if !KeyCanPerformOperation(key, jwk.KeyOpEncrypt) {
		return nil, nil, errors.New("key cannot perform the 'encrypt' operation")
	}
	if !KeyCanPerformAlgorithm(key, algorithm) {
		return nil, nil, fmt.Errorf("key cannot be used with algorithm '%s'", algorithm)
	}

	// Encrypt the data
	ciphertext, tag, err = internals.Encrypt(plaintext, algorithm, key, nonce, associatedData)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to encrypt data: %w", err)
	}
	return ciphertext, tag, nil
}

func (k LocalCryptoBaseComponent) Decrypt(parentCtx context.Context, ciphertext []byte, algorithm string, keyName string, nonce []byte, tag []byte, associatedData []byte) (plaintext []byte, err error) {
	// Retrieve the key
	key, err := k.RetrieveKeyFn(parentCtx, keyName)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve the key: %w", err)
	}

	// Check if the key can perform the operation
	if !KeyCanPerformOperation(key, jwk.KeyOpDecrypt) {
		return nil, errors.New("key cannot perform the 'decrypt' operation")
	}
	if !KeyCanPerformAlgorithm(key, algorithm) {
		return nil, fmt.Errorf("key cannot be used with algorithm '%s'", algorithm)
	}

	// Decrypt the data
	plaintext, err = internals.Decrypt(ciphertext, algorithm, key, nonce, tag, associatedData)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt data: %w", err)
	}
	return plaintext, nil
}

func (k LocalCryptoBaseComponent) WrapKey(parentCtx context.Context, plaintextKey jwk.Key, algorithm string, keyName string, nonce []byte, associatedData []byte) (wrappedKey []byte, tag []byte, err error) {
	// Serialize the plaintextKey
	plaintext, err := internals.SerializeKey(plaintextKey)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot serialize key: %w", err)
	}

	// Retrieve the key encryption key
	kek, err := k.RetrieveKeyFn(parentCtx, keyName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to retrieve the key encryption key: %w", err)
	}

	// Check if the key can perform the operation
	if !KeyCanPerformOperation(kek, jwk.KeyOpWrapKey) {
		return nil, nil, errors.New("key cannot perform the 'wrapKey' operation")
	}
	if !KeyCanPerformAlgorithm(kek, algorithm) {
		return nil, nil, fmt.Errorf("key cannot be used with algorithm '%s'", algorithm)
	}

	// Encrypt the data
	wrappedKey, tag, err = internals.Encrypt(plaintext, algorithm, kek, nonce, associatedData)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to encrypt data: %w", err)
	}
	return wrappedKey, tag, nil
}

func (k LocalCryptoBaseComponent) UnwrapKey(parentCtx context.Context, wrappedKey []byte, algorithm string, keyName string, nonce []byte, tag []byte, associatedData []byte) (plaintextKey jwk.Key, err error) {
	// Retrieve the key encryption key
	kek, err := k.RetrieveKeyFn(parentCtx, keyName)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve the key encryption key: %w", err)
	}

	// Check if the key can perform the operation
	if !KeyCanPerformOperation(kek, jwk.KeyOpUnwrapKey) {
		return nil, errors.New("key cannot perform the 'unwrapKey' operation")
	}
	if !KeyCanPerformAlgorithm(kek, algorithm) {
		return nil, fmt.Errorf("key cannot be used with algorithm '%s'", algorithm)
	}

	// Decrypt the data
	plaintext, err := internals.Decrypt(wrappedKey, algorithm, kek, nonce, tag, associatedData)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt data: %w", err)
	}

	// We allow wrapping/unwrapping only symmetric keys, so no need to try and decode an ASN.1 DER-encoded sequence
	plaintextKey, err = jwk.FromRaw(plaintext)
	if err != nil {
		return nil, fmt.Errorf("failed to create JWK from raw key: %w", err)
	}

	return plaintextKey, nil
}

func (k LocalCryptoBaseComponent) Sign(parentCtx context.Context, digest []byte, algorithm string, keyName string) (signature []byte, err error) {
	// Retrieve the key
	key, err := k.RetrieveKeyFn(parentCtx, keyName)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve the key: %w", err)
	}

	// Check if the key can perform the operation
	if !KeyCanPerformOperation(key, jwk.KeyOpSign) {
		return nil, errors.New("key cannot perform the 'sign' operation")
	}
	if !KeyCanPerformAlgorithm(key, algorithm) {
		return nil, fmt.Errorf("key cannot be used with algorithm '%s'", algorithm)
	}

	// Sign the message
	signature, err = internals.SignPrivateKey(digest, algorithm, key)
	if err != nil {
		return nil, fmt.Errorf("failed to sign message: %w", err)
	}
	return signature, nil
}

func (k LocalCryptoBaseComponent) Verify(parentCtx context.Context, digest []byte, signature []byte, algorithm string, keyName string) (valid bool, err error) {
	// Retrieve the key
	key, err := k.RetrieveKeyFn(parentCtx, keyName)
	if err != nil {
		return false, fmt.Errorf("failed to retrieve the key: %w", err)
	}

	// Check if the key can perform the operation
	if !KeyCanPerformOperation(key, jwk.KeyOpVerify) {
		return false, errors.New("key cannot perform the 'verify' operation")
	}
	if !KeyCanPerformAlgorithm(key, algorithm) {
		return false, fmt.Errorf("key cannot be used with algorithm '%s'", algorithm)
	}

	// Verify the signature
	valid, err = internals.VerifyPublicKey(digest, signature, algorithm, key)
	if err != nil {
		return false, fmt.Errorf("failed to validate the signature: %w", err)
	}
	return valid, nil
}

func (k LocalCryptoBaseComponent) SupportedEncryptionAlgorithms() []string {
	supportedAlgsOnce.Do(populateSupportedAlgs)
	return supportedEncryptionAlgorithms
}

func (k LocalCryptoBaseComponent) SupportedSignatureAlgorithms() []string {
	supportedAlgsOnce.Do(populateSupportedAlgs)
	return supportedSignatureAlgorithms
}

func populateSupportedAlgs() {
	symmetric := internals.SupportedSymmetricAlgorithms()
	asymmetric := internals.SupportedAsymmetricAlgorithms()
	supportedEncryptionAlgorithms = make([]string, len(symmetric)+len(asymmetric))
	copy(supportedEncryptionAlgorithms[0:len(symmetric)], symmetric)
	copy(supportedEncryptionAlgorithms[len(symmetric):], asymmetric)

	supportedSignatureAlgorithms = internals.SupportedSignatureAlgorithms()
}
