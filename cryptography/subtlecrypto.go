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
	"io"

	"github.com/lestrrat-go/jwx/v2/jwk"

	"github.com/dapr/components-contrib/metadata"
)

// SubtleCrypto offers an interface to perform low-level ("subtle") cryptographic operations with keys stored in a vault.
//
//nolint:interfacebloat
type SubtleCrypto interface {
	metadata.ComponentWithMetadata

	SubtleCryptoAlgorithms

	// Init the component.
	Init(ctx context.Context, metadata Metadata) error

	// GetKey returns the public part of a key stored in the vault.
	// This method returns an error if the key is symmetric.
	GetKey(ctx context.Context,
		// Name (or name/version) of the key to use in the key vault
		keyName string,
	) (
		// Object containing the public key
		pubKey jwk.Key,
		err error,
	)

	// Encrypt a small message and returns the ciphertext.
	Encrypt(ctx context.Context,
		// Input plaintext
		plaintext []byte,
		// Encryption algorithm to use
		algorithm string,
		// Name (or name/version) of the key to use in the key vault
		keyName string,
		// Nonce / initialization vector
		// Ignored with asymmetric ciphers
		nonce []byte,
		// Associated Data when using AEAD ciphers
		// Optional, can be nil
		associatedData []byte,
	) (
		// Encrypted ciphertext
		ciphertext []byte,
		// Authentication tag
		// This is nil when not using an authenticated cipher
		tag []byte,
		err error,
	)

	// Decrypt a small message and returns the plaintext.
	Decrypt(ctx context.Context,
		// Input ciphertext
		ciphertext []byte,
		// Encryption algorithm to use
		algorithm string,
		// Name (or name/version) of the key to use in the key vault
		keyName string,
		// Nonce / initialization vector
		// Ignored with asymmetric ciphers
		nonce []byte,
		// Authentication tag
		// Ignored when not using an authenticated cipher
		tag []byte,
		// Associated Data when using AEAD ciphers
		// Optional, can be nil
		associatedData []byte,
	) (
		// Decrypted plaintext
		plaintext []byte,
		err error,
	)

	// WrapKey wraps a key.
	WrapKey(ctx context.Context,
		// Key to wrap as Key object
		plaintextKey jwk.Key,
		// Encryption algorithm to use
		algorithm string,
		// Name (or name/version) of the key to use in the key vault
		keyName string,
		// Nonce / initialization vector
		// Ignored with asymmetric ciphers
		nonce []byte,
		// Associated Data when using AEAD ciphers
		// Optional, can be nil
		associatedData []byte,
	) (
		// Wrapped key
		wrappedKey []byte,
		// Authentication tag
		// This is nil when not using an authenticated cipher
		tag []byte,
		err error,
	)

	// UnwrapKey unwraps a key.
	// The consumer needs to unserialize the key in the correct format.
	UnwrapKey(ctx context.Context,
		// Wrapped key
		wrappedKey []byte,
		// Encryption algorithm to use
		algorithm string,
		// Name (or name/version) of the key to use in the key vault
		keyName string,
		// Nonce / initialization vector
		// Ignored with asymmetric ciphers
		nonce []byte,
		// Authentication tag
		// Ignored when not using an authenticated cipher
		tag []byte,
		// Associated Data when using AEAD ciphers
		// Optional, can be nil
		associatedData []byte,
	) (
		// Plaintext key
		plaintextKey jwk.Key,
		err error,
	)

	// Sign a digest.
	Sign(ctx context.Context,
		// Digest to sign
		digest []byte,
		// Signing algorithm to use
		algorithm string,
		// Name (or name/version) of the key to use in the key vault
		// The key must be asymmetric
		keyName string,
	) (
		// Signature that was computed
		signature []byte,
		err error,
	)

	// Verify a signature.
	Verify(ctx context.Context,
		// Digest of the message
		digest []byte,
		// Signature to verify
		signature []byte,
		// Signing algorithm to use
		algorithm string,
		// Name (or name/version) of the key to use in the key vault
		// The key must be asymmetric
		keyName string,
	) (
		// True if the signature is valid
		valid bool,
		err error,
	)

	io.Closer
}

// SubtleCryptoAlgorithms is an extension to SubtleCrypto that includes methods to return information on the supported algorithms.
type SubtleCryptoAlgorithms interface {
	SupportedEncryptionAlgorithms() []string
	SupportedSignatureAlgorithms() []string
}
