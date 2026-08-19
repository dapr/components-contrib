/*
Copyright 2026 The Dapr Authors
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

package kms

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	kms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/googleapis/gax-go/v2"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"google.golang.org/api/option"

	contribCrypto "github.com/dapr/components-contrib/crypto"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	internals "github.com/dapr/kit/crypto"
	"github.com/dapr/kit/logger"
)

// AlgorithmSymmetric is the identifier for Google's symmetric encryption keys. Those keys have no
// JWA equivalent because Cloud KMS manages the nonce and the authentication tag internally.
const AlgorithmSymmetric = "GOOGLE_SYMMETRIC_ENCRYPTION"

var (
	encryptionAlgorithms = []string{
		AlgorithmSymmetric,
		internals.Algorithm_RSA_OAEP,     // RSA_DECRYPT_OAEP_*_SHA1
		internals.Algorithm_RSA_OAEP_256, // RSA_DECRYPT_OAEP_*_SHA256
		internals.Algorithm_RSA_OAEP_512, // RSA_DECRYPT_OAEP_4096_SHA512
	}
	signatureAlgorithms = []string{
		internals.Algorithm_RS256, // RSA_SIGN_PKCS1_*_SHA256
		internals.Algorithm_RS512, // RSA_SIGN_PKCS1_4096_SHA512
		internals.Algorithm_PS256, // RSA_SIGN_PSS_*_SHA256
		internals.Algorithm_PS512, // RSA_SIGN_PSS_4096_SHA512
		internals.Algorithm_ES256, // EC_SIGN_P256_SHA256
		internals.Algorithm_ES384, // EC_SIGN_P384_SHA384
		internals.Algorithm_EdDSA, // EC_SIGN_ED25519
	}
)

// Subset of the Cloud KMS client used by this component, so tests can replace it.
type kmsClient interface {
	GetPublicKey(ctx context.Context, req *kmspb.GetPublicKeyRequest, opts ...gax.CallOption) (*kmspb.PublicKey, error)
	Encrypt(ctx context.Context, req *kmspb.EncryptRequest, opts ...gax.CallOption) (*kmspb.EncryptResponse, error)
	Decrypt(ctx context.Context, req *kmspb.DecryptRequest, opts ...gax.CallOption) (*kmspb.DecryptResponse, error)
	AsymmetricDecrypt(ctx context.Context, req *kmspb.AsymmetricDecryptRequest, opts ...gax.CallOption) (*kmspb.AsymmetricDecryptResponse, error)
	AsymmetricSign(ctx context.Context, req *kmspb.AsymmetricSignRequest, opts ...gax.CallOption) (*kmspb.AsymmetricSignResponse, error)
	Close() error
}

type kmsCrypto struct {
	client   kmsClient
	md       kmsMetadata
	keyCache *contribCrypto.PubKeyCache
	logger   logger.Logger
}

// NewGCPKMSCrypto returns a new GCP Cloud KMS crypto provider.
func NewGCPKMSCrypto(logger logger.Logger) contribCrypto.SubtleCrypto {
	return &kmsCrypto{
		logger: logger,
	}
}

// Init creates a Cloud KMS client.
func (k *kmsCrypto) Init(ctx context.Context, metadata contribCrypto.Metadata) error {
	err := k.md.InitWithMetadata(metadata)
	if err != nil {
		return fmt.Errorf("failed to load metadata: %w", err)
	}

	k.keyCache = contribCrypto.NewPubKeyCache(k.getKeyCacheFn)

	var opts []option.ClientOption
	if k.md.hasExplicitCredentials() {
		// Explicit authentication, with the service account credentials in the component metadata
		var creds []byte
		creds, err = json.Marshal(k.md)
		if err != nil {
			return fmt.Errorf("failed to encode the credentials: %w", err)
		}
		opts = append(opts, option.WithCredentialsJSON(creds))
	}
	// Otherwise, implicit authentication with GCP Application Default Credentials (ADC):
	// https://cloud.google.com/docs/authentication/application-default-credentials#order

	k.client, err = kms.NewKeyManagementClient(ctx, opts...)
	if err != nil {
		return fmt.Errorf("failed to create the Cloud KMS client: %w", err)
	}

	return nil
}

// Features returns the features available in this crypto provider.
func (k *kmsCrypto) Features() []contribCrypto.Feature {
	return []contribCrypto.Feature{} // No Feature supported.
}

// GetKey returns the public part of a key stored in Cloud KMS.
// This method returns an error if the key is symmetric.
// The key argument must be in the format "name/version".
func (k *kmsCrypto) GetKey(parentCtx context.Context, key string) (pubKey jwk.Key, err error) {
	kid := newKeyID(key)

	if kid.Cacheable() {
		return k.keyCache.GetKey(parentCtx, key)
	}

	return k.getPublicKey(parentCtx, kid)
}

func (k *kmsCrypto) getPublicKey(parentCtx context.Context, kid keyID) (jwk.Key, error) {
	name, err := k.md.cryptoKeyVersionPath(kid)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(parentCtx, k.md.RequestTimeout)
	res, err := k.client.GetPublicKey(ctx, &kmspb.GetPublicKeyRequest{Name: name})
	cancel()
	if err != nil {
		return nil, fmt.Errorf("failed to get the public key from Cloud KMS: %w", err)
	}
	if res.GetPem() == "" {
		return nil, errors.New("response from Cloud KMS does not contain a public key: the key may be symmetric")
	}

	pubKey, err := jwk.ParseKey([]byte(res.GetPem()), jwk.WithPEM(true))
	if err != nil {
		return nil, fmt.Errorf("failed to parse the public key returned by Cloud KMS: %w", err)
	}

	return pubKey, nil
}

// Handler for the getKeyCacheFn method
func (k *kmsCrypto) getKeyCacheFn(ctx context.Context, key string) func(resolve func(jwk.Key), reject func(error)) {
	kid := newKeyID(key)
	return func(resolve func(jwk.Key), reject func(error)) {
		pk, err := k.getPublicKey(ctx, kid)
		if err != nil {
			reject(err)
			return
		}
		resolve(pk)
	}
}

// Encrypt a small message and returns the ciphertext.
// Symmetric keys are used through Cloud KMS, while data encrypted with an asymmetric key is
// encrypted locally with the public key, because Cloud KMS only offers asymmetric decryption.
func (k *kmsCrypto) Encrypt(parentCtx context.Context, plaintext []byte, algorithm string, key string, nonce []byte, associatedData []byte) (ciphertext []byte, tag []byte, err error) {
	switch algorithm {
	case AlgorithmSymmetric:
		if len(nonce) > 0 {
			return nil, nil, errors.New("nonce is not supported with symmetric keys: Cloud KMS manages the nonce internally")
		}

		kid := newKeyID(key)
		ctx, cancel := context.WithTimeout(parentCtx, k.md.RequestTimeout)
		res, rErr := k.client.Encrypt(ctx, &kmspb.EncryptRequest{
			Name:                        k.md.cryptoKeyPath(kid),
			Plaintext:                   plaintext,
			AdditionalAuthenticatedData: associatedData,
		})
		cancel()
		if rErr != nil {
			return nil, nil, fmt.Errorf("error from Cloud KMS: %w", rErr)
		}
		// The authentication tag is included in the ciphertext returned by Cloud KMS
		return res.GetCiphertext(), nil, nil

	case internals.Algorithm_RSA_OAEP, internals.Algorithm_RSA_OAEP_256, internals.Algorithm_RSA_OAEP_512:
		if len(associatedData) > 0 {
			// Cloud KMS does not accept an OAEP label when decrypting, so data encrypted with one could never be decrypted
			return nil, nil, errors.New("associated data is not supported with asymmetric keys")
		}

		pk, pkErr := k.GetKey(parentCtx, key)
		if pkErr != nil {
			return nil, nil, fmt.Errorf("failed to retrieve public key: %w", pkErr)
		}

		ciphertext, err = internals.EncryptPublicKey(plaintext, algorithm, pk, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to encrypt data: %w", err)
		}
		return ciphertext, nil, nil

	default:
		return nil, nil, fmt.Errorf("invalid algorithm: %s", algorithm)
	}
}

// Decrypt a small message and returns the plaintext.
func (k *kmsCrypto) Decrypt(parentCtx context.Context, ciphertext []byte, algorithm string, key string, nonce []byte, tag []byte, associatedData []byte) (plaintext []byte, err error) {
	kid := newKeyID(key)

	switch algorithm {
	case AlgorithmSymmetric:
		// Cloud KMS selects the key version from the ciphertext, so decryption always uses the crypto key
		ctx, cancel := context.WithTimeout(parentCtx, k.md.RequestTimeout)
		res, rErr := k.client.Decrypt(ctx, &kmspb.DecryptRequest{
			Name:                        k.md.cryptoKeyPath(kid),
			Ciphertext:                  ciphertext,
			AdditionalAuthenticatedData: associatedData,
		})
		cancel()
		if rErr != nil {
			return nil, fmt.Errorf("error from Cloud KMS: %w", rErr)
		}
		return res.GetPlaintext(), nil

	case internals.Algorithm_RSA_OAEP, internals.Algorithm_RSA_OAEP_256, internals.Algorithm_RSA_OAEP_512:
		if len(associatedData) > 0 {
			return nil, errors.New("associated data is not supported with asymmetric keys")
		}

		name, nErr := k.md.cryptoKeyVersionPath(kid)
		if nErr != nil {
			return nil, nErr
		}

		ctx, cancel := context.WithTimeout(parentCtx, k.md.RequestTimeout)
		res, rErr := k.client.AsymmetricDecrypt(ctx, &kmspb.AsymmetricDecryptRequest{
			Name:       name,
			Ciphertext: ciphertext,
		})
		cancel()
		if rErr != nil {
			return nil, fmt.Errorf("error from Cloud KMS: %w", rErr)
		}
		return res.GetPlaintext(), nil

	default:
		return nil, fmt.Errorf("invalid algorithm: %s", algorithm)
	}
}

// WrapKey wraps a symmetric key.
func (k *kmsCrypto) WrapKey(ctx context.Context, plaintextKey jwk.Key, algorithm string, key string, nonce []byte, associatedData []byte) (wrappedKey []byte, tag []byte, err error) {
	// Like the other vault-backed components, only symmetric keys can be wrapped, so the unwrapped
	// key can be reconstructed without guessing how it was serialized
	if plaintextKey.KeyType() != jwa.OctetSeq {
		return nil, nil, errors.New("cannot wrap asymmetric keys")
	}
	plaintext, err := internals.SerializeKey(plaintextKey)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot serialize key: %w", err)
	}

	return k.Encrypt(ctx, plaintext, algorithm, key, nonce, associatedData)
}

// UnwrapKey unwraps a key.
func (k *kmsCrypto) UnwrapKey(ctx context.Context, wrappedKey []byte, algorithm string, key string, nonce []byte, tag []byte, associatedData []byte) (plaintextKey jwk.Key, err error) {
	plaintext, err := k.Decrypt(ctx, wrappedKey, algorithm, key, nonce, tag, associatedData)
	if err != nil {
		return nil, err
	}

	plaintextKey, err = jwk.FromRaw(plaintext)
	if err != nil {
		return nil, fmt.Errorf("failed to create JWK from raw key: %w", err)
	}

	return plaintextKey, nil
}

// Sign a digest.
// The key argument must be in the format "name/version".
func (k *kmsCrypto) Sign(parentCtx context.Context, digest []byte, algorithm string, key string) (signature []byte, err error) {
	req, err := signRequestDigest(digest, algorithm)
	if err != nil {
		return nil, err
	}

	req.Name, err = k.md.cryptoKeyVersionPath(newKeyID(key))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(parentCtx, k.md.RequestTimeout)
	res, err := k.client.AsymmetricSign(ctx, req)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("error from Cloud KMS: %w", err)
	}
	if len(res.GetSignature()) == 0 {
		return nil, errors.New("response from Cloud KMS does not contain a signature")
	}

	return res.GetSignature(), nil
}

// Builds the signing request for the given algorithm, validating the length of the digest.
// Cloud KMS signs Ed25519 messages directly, which matches how the rest of Dapr treats EdDSA.
func signRequestDigest(digest []byte, algorithm string) (*kmspb.AsymmetricSignRequest, error) {
	switch algorithm {
	case internals.Algorithm_EdDSA:
		if len(digest) == 0 {
			return nil, errors.New("message to sign is empty")
		}
		return &kmspb.AsymmetricSignRequest{Data: digest}, nil

	case internals.Algorithm_ES256, internals.Algorithm_RS256, internals.Algorithm_PS256:
		if len(digest) != 32 {
			return nil, fmt.Errorf("digest for algorithm %s must be 32 bytes long, but it's %d", algorithm, len(digest))
		}
		return &kmspb.AsymmetricSignRequest{
			Digest: &kmspb.Digest{Digest: &kmspb.Digest_Sha256{Sha256: digest}},
		}, nil

	case internals.Algorithm_ES384:
		if len(digest) != 48 {
			return nil, fmt.Errorf("digest for algorithm %s must be 48 bytes long, but it's %d", algorithm, len(digest))
		}
		return &kmspb.AsymmetricSignRequest{
			Digest: &kmspb.Digest{Digest: &kmspb.Digest_Sha384{Sha384: digest}},
		}, nil

	case internals.Algorithm_RS512, internals.Algorithm_PS512:
		if len(digest) != 64 {
			return nil, fmt.Errorf("digest for algorithm %s must be 64 bytes long, but it's %d", algorithm, len(digest))
		}
		return &kmspb.AsymmetricSignRequest{
			Digest: &kmspb.Digest{Digest: &kmspb.Digest_Sha512{Sha512: digest}},
		}, nil

	default:
		return nil, fmt.Errorf("invalid algorithm: %s", algorithm)
	}
}

// Verify a signature.
// Cloud KMS has no verification API for asymmetric keys, so the signature is verified locally
// with the public key.
// The key argument must be in the format "name/version".
func (k *kmsCrypto) Verify(parentCtx context.Context, digest []byte, signature []byte, algorithm string, key string) (valid bool, err error) {
	pk, err := k.GetKey(parentCtx, key)
	if err != nil {
		return false, fmt.Errorf("failed to retrieve public key: %w", err)
	}

	valid, err = internals.VerifyPublicKey(digest, signature, algorithm, pk)
	if err != nil {
		return false, fmt.Errorf("failed to verify signature: %w", err)
	}

	return valid, nil
}

func (k *kmsCrypto) Close() error {
	if k.client == nil {
		return nil
	}
	return k.client.Close()
}

func (*kmsCrypto) SupportedEncryptionAlgorithms() []string {
	return encryptionAlgorithms
}

func (*kmsCrypto) SupportedSignatureAlgorithms() []string {
	return signatureAlgorithms
}

func (*kmsCrypto) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := kmsMetadata{}
	_ = contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.CryptoType)
	return
}
