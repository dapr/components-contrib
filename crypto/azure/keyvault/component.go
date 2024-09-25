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

package keyvault

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"

	contribCrypto "github.com/dapr/components-contrib/crypto"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	internals "github.com/dapr/kit/crypto"
	"github.com/dapr/kit/logger"
)

var errKeyNotFound = errors.New("key not found in the vault")

type keyvaultCrypto struct {
	keyCache    *contribCrypto.PubKeyCache
	md          keyvaultMetadata
	vaultClient *azkeys.Client
	logger      logger.Logger
}

// NewAzureKeyvaultCrypto returns a new Azure Key Vault crypto provider.
func NewAzureKeyvaultCrypto(logger logger.Logger) contribCrypto.SubtleCrypto {
	return &keyvaultCrypto{
		logger: logger,
	}
}

// Init creates a Azure Key Vault client.
func (k *keyvaultCrypto) Init(_ context.Context, metadata contribCrypto.Metadata) error {
	// Convert from data from the Azure SDK, which returns a slice, into a map
	// We perform the initialization here, lazily, when the first component of this kind is initialized
	// (These functions do not make network calls)
	algsParsed.Do(func() {
		listEncryption := azkeys.PossibleEncryptionAlgorithmValues()
		validEncryptionAlgs = make(map[string]struct{}, len(listEncryption))
		encryptionAlgsList = make([]string, len(listEncryption))
		for i, v := range listEncryption {
			validEncryptionAlgs[string(v)] = struct{}{}
			encryptionAlgsList[i] = string(v)
		}

		listSignature := azkeys.PossibleSignatureAlgorithmValues()
		validSignatureAlgs = make(map[string]struct{}, len(listSignature))
		signatureAlgsList = make([]string, len(listSignature))
		for i, v := range listSignature {
			validSignatureAlgs[string(v)] = struct{}{}
			signatureAlgsList[i] = string(v)
		}
	})

	// Init the metadata
	err := k.md.InitWithMetadata(metadata)
	if err != nil {
		return fmt.Errorf("failed to load metadata: %w", err)
	}

	// Create a cache for keys
	k.keyCache = contribCrypto.NewPubKeyCache(k.getKeyCacheFn)

	// Init the Azure SDK client
	k.vaultClient, err = azkeys.NewClient(k.getVaultURI(), k.md.cred, &azkeys.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Telemetry: policy.TelemetryOptions{
				ApplicationID: "dapr-" + logger.DaprVersion,
			},
		},
	})
	if err != nil {
		return err
	}

	return nil
}

// Features returns the features available in this crypto provider.
func (k *keyvaultCrypto) Features() []contribCrypto.Feature {
	return []contribCrypto.Feature{} // No Feature supported.
}

// GetKey returns the public part of a key stored in the vault.
// This method returns an error if the key is symmetric.
// The key argument can be in the format "name" or "name/version".
func (k *keyvaultCrypto) GetKey(parentCtx context.Context, key string) (pubKey jwk.Key, err error) {
	kid := newKeyID(key)

	// If the key is cacheable, get it from the cache
	if kid.Cacheable() {
		return k.keyCache.GetKey(parentCtx, key)
	}

	return k.getKeyFromVault(parentCtx, kid)
}

func (k *keyvaultCrypto) getKeyFromVault(parentCtx context.Context, kid keyID) (pubKey jwk.Key, err error) {
	ctx, cancel := context.WithTimeout(parentCtx, k.md.RequestTimeout)
	res, err := k.vaultClient.GetKey(ctx, kid.Name, kid.Version, nil)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("failed to get key from Key Vault: %w", err)
	}

	return KeyBundleToKey(&res.KeyBundle)
}

// Handler for the getKeyCacheFn method
func (k *keyvaultCrypto) getKeyCacheFn(ctx context.Context, key string) func(resolve func(jwk.Key), reject func(error)) {
	kid := newKeyID(key)
	return func(resolve func(jwk.Key), reject func(error)) {
		pk, err := k.getKeyFromVault(ctx, kid)
		if err != nil {
			reject(err)
			return
		}
		resolve(pk)
	}
}

// Encrypt a small message and returns the ciphertext.
// The key argument can be in the format "name" or "name/version".
func (k *keyvaultCrypto) Encrypt(parentCtx context.Context, plaintext []byte, algorithmStr string, key string, nonce []byte, associatedData []byte) (ciphertext []byte, tag []byte, err error) {
	kid := newKeyID(key)

	algorithm := GetJWKEncryptionAlgorithm(algorithmStr)
	if algorithm == nil {
		return nil, nil, fmt.Errorf("invalid algorithm: %s", algorithmStr)
	}

	// Encrypting with symmetric or non-cacheable keys must happen in the vault
	if !kid.Cacheable() || !IsAlgorithmAsymmetric(*algorithm) {
		return k.encryptInVault(parentCtx, plaintext, algorithm, kid, nonce, associatedData)
	}

	// Using a cacheable, asymmetric key, we can encrypt the data directly here
	pk, err := k.keyCache.GetKey(parentCtx, key)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to retrieve public key: %w", err)
	}

	// If the key has expired, we cannot use that to encrypt data
	if dpk, ok := pk.(*contribCrypto.Key); ok && !dpk.IsValid() {
		return nil, nil, errors.New("the key is outside of its time validity bounds")
	}

	ciphertext, err = internals.EncryptPublicKey(plaintext, algorithmStr, pk, associatedData)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to encrypt data: %w", err)
	}
	return ciphertext, nil, nil
}

func (k *keyvaultCrypto) encryptInVault(parentCtx context.Context, plaintext []byte, algorithm *azkeys.EncryptionAlgorithm, kid keyID, nonce []byte, associatedData []byte) (ciphertext []byte, tag []byte, err error) {
	ctx, cancel := context.WithTimeout(parentCtx, k.md.RequestTimeout)
	res, err := k.vaultClient.Encrypt(ctx, kid.Name, kid.Version, azkeys.KeyOperationParameters{
		Algorithm:                   algorithm,
		Value:                       plaintext,
		IV:                          nonce,
		AdditionalAuthenticatedData: associatedData,
	}, nil)
	cancel()
	if err != nil {
		return nil, nil, fmt.Errorf("error from Key Vault: %w", err)
	}

	if res.Result == nil {
		return nil, nil, errors.New("response from Key Vault does not contain a valid ciphertext")
	}

	return res.Result, res.AuthenticationTag, nil
}

// Decrypt a small message and returns the plaintext.
// The key argument can be in the format "name" or "name/version".
func (k *keyvaultCrypto) Decrypt(parentCtx context.Context, ciphertext []byte, algorithmStr string, key string, nonce []byte, tag []byte, associatedData []byte) (plaintext []byte, err error) {
	kid := newKeyID(key)

	algorithm := GetJWKEncryptionAlgorithm(algorithmStr)
	if algorithm == nil {
		return nil, fmt.Errorf("invalid algorithm: %s", algorithmStr)
	}

	ctx, cancel := context.WithTimeout(parentCtx, k.md.RequestTimeout)
	res, err := k.vaultClient.Decrypt(ctx, kid.Name, kid.Version, azkeys.KeyOperationParameters{
		Algorithm:                   algorithm,
		Value:                       ciphertext,
		IV:                          nonce,
		AuthenticationTag:           tag,
		AdditionalAuthenticatedData: associatedData,
	}, nil)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("error from Key Vault: %w", err)
	}

	if res.Result == nil {
		return nil, errors.New("response from Key Vault does not contain a valid plaintext")
	}

	return res.Result, nil
}

// WrapKey wraps a symmetric key.
// The key argument can be in the format "name" or "name/version".
func (k *keyvaultCrypto) WrapKey(parentCtx context.Context, plaintextKey jwk.Key, algorithmStr string, key string, nonce []byte, associatedData []byte) (wrappedKey []byte, tag []byte, err error) {
	// Azure Key Vault does not support wrapping asymmetric keys
	if plaintextKey.KeyType() != jwa.OctetSeq {
		return nil, nil, errors.New("cannot wrap asymmetric keys")
	}
	plaintext, err := internals.SerializeKey(plaintextKey)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot serialize key: %w", err)
	}

	kid := newKeyID(key)

	algorithm := GetJWKEncryptionAlgorithm(algorithmStr)
	if algorithm == nil {
		return nil, nil, fmt.Errorf("invalid algorithm: %s", algorithmStr)
	}

	// Encrypting with symmetric or non-cacheable keys must happen in the vault
	if !kid.Cacheable() || !IsAlgorithmAsymmetric(*algorithm) {
		return k.wrapKeyInVault(parentCtx, plaintext, algorithm, kid, nonce, associatedData)
	}

	// Using a cacheable, asymmetric key, we can encrypt the data directly here
	pk, err := k.keyCache.GetKey(parentCtx, key)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to retrieve public key: %w", err)
	}

	// If the key has expired, we cannot use that to encrypt data
	if dpk, ok := pk.(*contribCrypto.Key); ok && !dpk.IsValid() {
		return nil, nil, errors.New("the key is outside of its time validity bounds")
	}

	wrappedKey, err = internals.EncryptPublicKey(plaintext, algorithmStr, pk, associatedData)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to wrap key: %w", err)
	}
	return wrappedKey, nil, nil
}

func (k *keyvaultCrypto) wrapKeyInVault(parentCtx context.Context, plaintextKey []byte, algorithm *azkeys.EncryptionAlgorithm, kid keyID, nonce []byte, associatedData []byte) (wrappedKey []byte, tag []byte, err error) {
	ctx, cancel := context.WithTimeout(parentCtx, k.md.RequestTimeout)
	res, err := k.vaultClient.WrapKey(ctx, kid.Name, kid.Version, azkeys.KeyOperationParameters{
		Algorithm:                   algorithm,
		Value:                       plaintextKey,
		IV:                          nonce,
		AdditionalAuthenticatedData: associatedData,
	}, nil)
	cancel()
	if err != nil {
		return nil, nil, fmt.Errorf("error from Key Vault: %w", err)
	}

	if res.Result == nil {
		return nil, nil, errors.New("response from Key Vault does not contain a valid wrapped key")
	}

	return res.Result, res.AuthenticationTag, nil
}

// UnwrapKey unwraps a key.
// The key argument can be in the format "name" or "name/version".
func (k *keyvaultCrypto) UnwrapKey(parentCtx context.Context, wrappedKey []byte, algorithmStr string, key string, nonce []byte, tag []byte, associatedData []byte) (plaintextKey jwk.Key, err error) {
	kid := newKeyID(key)

	algorithm := GetJWKEncryptionAlgorithm(algorithmStr)
	if algorithm == nil {
		return nil, fmt.Errorf("invalid algorithm: %s", algorithmStr)
	}

	ctx, cancel := context.WithTimeout(parentCtx, k.md.RequestTimeout)
	res, err := k.vaultClient.UnwrapKey(ctx, kid.Name, kid.Version, azkeys.KeyOperationParameters{
		Algorithm:                   algorithm,
		Value:                       wrappedKey,
		IV:                          nonce,
		AuthenticationTag:           tag,
		AdditionalAuthenticatedData: associatedData,
	}, nil)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("error from Key Vault: %w", err)
	}

	if res.Result == nil {
		return nil, errors.New("response from Key Vault does not contain a valid unwrapped key")
	}

	// Key Vault allows wrapping/unwrapping only symmetric keys, so no need to try and decode an ASN.1 DER-encoded sequence
	plaintextKey, err = jwk.FromRaw(res.Result)
	if err != nil {
		return nil, fmt.Errorf("failed to create JWK from raw key: %w", err)
	}

	return plaintextKey, nil
}

// Sign a digest.
// The key argument can be in the format "name" or "name/version".
func (k *keyvaultCrypto) Sign(parentCtx context.Context, digest []byte, algorithmStr string, key string) (signature []byte, err error) {
	kid := newKeyID(key)

	algorithm := GetJWKSignatureAlgorithm(algorithmStr)
	if algorithm == nil {
		return nil, fmt.Errorf("invalid algorithm: %s", algorithmStr)
	}

	ctx, cancel := context.WithTimeout(parentCtx, k.md.RequestTimeout)
	res, err := k.vaultClient.Sign(ctx, kid.Name, kid.Version, azkeys.SignParameters{
		Algorithm: algorithm,
		Value:     digest,
	}, nil)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("error from Key Vault: %w", err)
	}

	if res.Result == nil {
		return nil, errors.New("response from Key Vault does not contain a valid signature")
	}

	return res.Result, nil
}

// Verify a signature.
// The key argument can be in the format "name" or "name/version".
func (k *keyvaultCrypto) Verify(parentCtx context.Context, digest []byte, signature []byte, algorithmStr string, key string) (valid bool, err error) {
	kid := newKeyID(key)

	algorithm := GetJWKSignatureAlgorithm(algorithmStr)
	if algorithm == nil {
		return false, fmt.Errorf("invalid algorithm: %s", algorithmStr)
	}

	// Verifying with non-cacheable keys must happen in the vault
	if !kid.Cacheable() {
		return k.verifyInVault(parentCtx, digest, signature, algorithm, kid)
	}

	// Using a cacheable, asymmetric key, we can verify the data directly here
	pk, err := k.keyCache.GetKey(parentCtx, key)
	if err != nil {
		return false, fmt.Errorf("failed to retrieve public key: %w", err)
	}

	valid, err = internals.VerifyPublicKey(digest, signature, algorithmStr, pk)
	if err != nil {
		return false, fmt.Errorf("failed to verify signature: %w", err)
	}
	return valid, nil
}

func (k *keyvaultCrypto) verifyInVault(parentCtx context.Context, digest []byte, signature []byte, algorithm *azkeys.SignatureAlgorithm, kid keyID) (valid bool, err error) {
	ctx, cancel := context.WithTimeout(parentCtx, k.md.RequestTimeout)
	res, err := k.vaultClient.Verify(ctx, kid.Name, kid.Version, azkeys.VerifyParameters{
		Algorithm: algorithm,
		Digest:    digest,
		Signature: signature,
	}, nil)
	cancel()
	if err != nil {
		return false, fmt.Errorf("error from Key Vault: %w", err)
	}

	if res.Value == nil {
		return false, errors.New("response from Key Vault does not contain a valid response")
	}

	return *res.Value, nil
}

// getVaultURI returns Azure Key Vault URI.
func (k *keyvaultCrypto) getVaultURI() string {
	return fmt.Sprintf("https://%s.%s", k.md.VaultName, k.md.vaultDNSSuffix)
}

func (k *keyvaultCrypto) Close() error {
	return nil
}

func (keyvaultCrypto) SupportedEncryptionAlgorithms() []string {
	return encryptionAlgsList
}

func (keyvaultCrypto) SupportedSignatureAlgorithms() []string {
	return signatureAlgsList
}

func (keyvaultCrypto) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := keyvaultMetadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.CryptoType)
	return
}

type keyID struct {
	Version string
	Name    string
}

func newKeyID(val string) keyID {
	obj := keyID{}
	idx := strings.IndexRune(val, '/')
	// Can't be on position 0, because the key name must be at least 1 character
	if idx > 0 {
		obj.Version = val[idx+1:]
		obj.Name = val[:idx]
	} else {
		obj.Name = val
	}
	return obj
}

// Cacheable returns true if the key can be cached locally.
func (id keyID) Cacheable() bool {
	switch strings.ToLower(id.Version) {
	case "", "latest":
		return false
	default:
		return true
	}
}
