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

package keyvault

import (
	"context"
	stdcrypto "crypto"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/keyvault/azkeys"
	"github.com/chebyrash/promise"

	"github.com/dapr/components-contrib/crypto"
	azauth "github.com/dapr/components-contrib/internal/authentication/azure"
	internals "github.com/dapr/components-contrib/internal/crypto"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

const (
	requestTimeout = 30 * time.Second
)

type keyvaultCrypto struct {
	vaultName      string
	vaultClient    *azkeys.Client
	vaultDNSSuffix string

	pubKeys     map[string]*promise.Promise[stdcrypto.PublicKey]
	pubKeysLock *sync.Mutex
	logger      logger.Logger
}

// NewAzureKeyvaultCrypto returns a new Azure Key Vault crypto provider.
func NewAzureKeyvaultCrypto(logger logger.Logger) crypto.SubtleCrypto {
	return &keyvaultCrypto{
		vaultName:   "",
		vaultClient: nil,
		logger:      logger,
		pubKeys:     map[string]*promise.Promise[stdcrypto.PublicKey]{},
		pubKeysLock: &sync.Mutex{},
	}
}

// Init creates a Azure Key Vault client.
func (k *keyvaultCrypto) Init(metadata secretstores.Metadata) error {
	// Initialization code
	settings, err := azauth.NewEnvironmentSettings("keyvault", metadata.Properties)
	if err != nil {
		return err
	}

	k.vaultName = settings.Values["vaultName"]
	k.vaultDNSSuffix = settings.AzureEnvironment.KeyVaultDNSSuffix

	cred, err := settings.GetTokenCredential()
	if err != nil {
		return err
	}
	k.vaultClient = azkeys.NewClient(k.getVaultURI(), cred, &azkeys.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Telemetry: policy.TelemetryOptions{
				ApplicationID: "dapr-" + logger.DaprVersion,
			},
		},
	})

	return nil
}

// Features returns the features available in this crypto provider.
func (k *keyvaultCrypto) Features() []crypto.Feature {
	return []crypto.Feature{} // No Feature supported.
}

func (k *keyvaultCrypto) GetKey(parentCtx context.Context, key string) (pubKey stdcrypto.PublicKey, err error) {
	timeoutPromise := promise.New(func(_ func(stdcrypto.PublicKey), reject func(error)) {
		<-parentCtx.Done()
		reject(parentCtx.Err())
	})

	// Check if the key is in the cache already
	k.pubKeysLock.Lock()
	p, ok := k.pubKeys[key]
	if ok {
		k.pubKeysLock.Unlock()
		return promise.Race(p, timeoutPromise).Await()
	}

	// Create a new promise, which resolves with a background context
	p = promise.New(k.getKeyFn(key))
	p = promise.Catch(p, func(err error) error {
		k.pubKeysLock.Lock()
		delete(k.pubKeys, key)
		k.pubKeysLock.Unlock()
		return err
	})
	k.pubKeys[key] = p
	k.pubKeysLock.Unlock()

	return promise.Race(p, timeoutPromise).Await()
}

func (k *keyvaultCrypto) getKeyFn(key string) func(resolve func(stdcrypto.PublicKey), reject func(error)) {
	return func(resolve func(stdcrypto.PublicKey), reject func(error)) {
		keyName, keyVersion := k.getKeyNameVersion(key)

		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		res, err := k.vaultClient.GetKey(ctx, keyName, keyVersion, nil)
		cancel()
		if err != nil {
			reject(fmt.Errorf("failed to get key from Key Vault: %w", err))
			return
		}

		if res.Key == nil {
			reject(fmt.Errorf("key not found: %s", key))
			return
		}

		pk, err := JSONWebKey{*res.Key}.Public()
		if err != nil {
			reject(fmt.Errorf("failed to extract public key as crypto.PublicKey: %w", err))
			return
		}
		resolve(pk)
	}
}

func (k *keyvaultCrypto) Encrypt(parentCtx context.Context, plaintext []byte, algorithmStr string, key string, nonce []byte, associatedData []byte) (ciphertext []byte, tag []byte, err error) {
	algorithm := GetJWKEncryptionAlgorithm(algorithmStr)
	if algorithm == nil {
		return nil, nil, fmt.Errorf("invalid algorithm: %s", algorithmStr)
	}

	// Encrypting with symmetric keys must happen in the vault
	if !IsAlgorithmAsymmetric(*algorithm) {
		return k.encryptInVault(parentCtx, plaintext, algorithm, key, nonce, associatedData)
	}

	// Using an asymmetric key, we can encrypt the data directly here
	pk, err := k.GetKey(parentCtx, key)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to retrieve public key: %w", err)
	}

	ciphertext, err = internals.EncryptPublicKey(plaintext, algorithmStr, pk, associatedData)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to encrypt data: %w", err)
	}
	return ciphertext, nil, nil
}

func (k *keyvaultCrypto) encryptInVault(parentCtx context.Context, plaintext []byte, algorithm *azkeys.JSONWebKeyEncryptionAlgorithm, key string, nonce []byte, associatedData []byte) (ciphertext []byte, tag []byte, err error) {
	keyName, keyVersion := k.getKeyNameVersion(key)

	ctx, cancel := context.WithTimeout(parentCtx, requestTimeout)
	res, err := k.vaultClient.Encrypt(ctx, keyName, keyVersion, azkeys.KeyOperationsParameters{
		Algorithm: algorithm,
		Value:     plaintext,
		IV:        nonce,
		AAD:       associatedData,
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

func (k *keyvaultCrypto) Decrypt(parentCtx context.Context, ciphertext []byte, algorithmStr string, key string, nonce []byte, tag []byte, associatedData []byte) (plaintext []byte, err error) {
	keyName, keyVersion := k.getKeyNameVersion(key)

	algorithm := GetJWKEncryptionAlgorithm(algorithmStr)
	if algorithm == nil {
		return nil, fmt.Errorf("invalid algorithm: %s", algorithmStr)
	}

	ctx, cancel := context.WithTimeout(parentCtx, requestTimeout)
	res, err := k.vaultClient.Decrypt(ctx, keyName, keyVersion, azkeys.KeyOperationsParameters{
		Algorithm: algorithm,
		Value:     ciphertext,
		IV:        nonce,
		Tag:       tag,
		AAD:       associatedData,
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

func (k *keyvaultCrypto) WrapKey(parentCtx context.Context, plaintextKey []byte, algorithmStr string, key string, nonce []byte, associatedData []byte) (wrappedKey []byte, tag []byte, err error) {
	algorithm := GetJWKEncryptionAlgorithm(algorithmStr)
	if algorithm == nil {
		return nil, nil, fmt.Errorf("invalid algorithm: %s", algorithmStr)
	}

	// Encrypting with symmetric keys must happen in the vault
	if !IsAlgorithmAsymmetric(*algorithm) {
		return k.wrapKeyInVault(parentCtx, plaintextKey, algorithm, key, nonce, associatedData)
	}

	// Using an asymmetric key, we can encrypt the data directly here
	pk, err := k.GetKey(parentCtx, key)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to retrieve public key: %w", err)
	}

	wrappedKey, err = internals.EncryptPublicKey(plaintextKey, algorithmStr, pk, associatedData)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to wrap key: %w", err)
	}
	return wrappedKey, nil, nil
}

func (k *keyvaultCrypto) wrapKeyInVault(parentCtx context.Context, plaintextKey []byte, algorithm *azkeys.JSONWebKeyEncryptionAlgorithm, key string, nonce []byte, associatedData []byte) (wrappedKey []byte, tag []byte, err error) {
	keyName, keyVersion := k.getKeyNameVersion(key)

	ctx, cancel := context.WithTimeout(parentCtx, requestTimeout)
	res, err := k.vaultClient.WrapKey(ctx, keyName, keyVersion, azkeys.KeyOperationsParameters{
		Algorithm: algorithm,
		Value:     plaintextKey,
		IV:        nonce,
		AAD:       associatedData,
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

func (k *keyvaultCrypto) UnwrapKey(parentCtx context.Context, wrappedKey []byte, algorithmStr string, key string, nonce []byte, tag []byte, associatedData []byte) (plaintextKey []byte, err error) {
	keyName, keyVersion := k.getKeyNameVersion(key)

	algorithm := GetJWKEncryptionAlgorithm(algorithmStr)
	if algorithm == nil {
		return nil, fmt.Errorf("invalid algorithm: %s", algorithmStr)
	}

	ctx, cancel := context.WithTimeout(parentCtx, requestTimeout)
	res, err := k.vaultClient.UnwrapKey(ctx, keyName, keyVersion, azkeys.KeyOperationsParameters{
		Algorithm: algorithm,
		Value:     wrappedKey,
		IV:        nonce,
		Tag:       tag,
		AAD:       associatedData,
	}, nil)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("error from Key Vault: %w", err)
	}

	if res.Result == nil {
		return nil, errors.New("response from Key Vault does not contain a valid unwrapped key")
	}

	return res.Result, nil
}

func (k *keyvaultCrypto) Sign(parentCtx context.Context, digest []byte, algorithmStr string, key string) (signature []byte, err error) {
	keyName, keyVersion := k.getKeyNameVersion(key)

	algorithm := GetJWKSignatureAlgorithm(algorithmStr)
	if algorithm == nil {
		return nil, fmt.Errorf("invalid algorithm: %s", algorithmStr)
	}

	ctx, cancel := context.WithTimeout(parentCtx, requestTimeout)
	res, err := k.vaultClient.Sign(ctx, keyName, keyVersion, azkeys.SignParameters{
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

func (k *keyvaultCrypto) Verify(parentCtx context.Context, digest []byte, signature []byte, algorithmStr string, key string) (valid bool, err error) {
	algorithm := GetJWKSignatureAlgorithm(algorithmStr)
	if algorithm == nil {
		return false, fmt.Errorf("invalid algorithm: %s", algorithmStr)
	}

	// Encrypting with symmetric keys must happen in the vault
	if !IsAlgorithmAsymmetric(*algorithm) {
		return k.verifyInVault(parentCtx, digest, signature, algorithm, key)
	}

	// Using an asymmetric key, we can encrypt the data directly here
	pk, err := k.GetKey(parentCtx, key)
	if err != nil {
		return false, fmt.Errorf("failed to retrieve public key: %w", err)
	}

	valid, err = internals.VerifyPublicKey(digest, signature, algorithmStr, pk)
	if err != nil {
		return false, fmt.Errorf("failed to verify signature: %w", err)
	}
	return valid, nil
}

func (k *keyvaultCrypto) verifyInVault(parentCtx context.Context, digest []byte, signature []byte, algorithm *azkeys.JSONWebKeySignatureAlgorithm, key string) (valid bool, err error) {
	keyName, keyVersion := k.getKeyNameVersion(key)

	ctx, cancel := context.WithTimeout(parentCtx, requestTimeout)
	res, err := k.vaultClient.Verify(ctx, keyName, keyVersion, azkeys.VerifyParameters{
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
	return fmt.Sprintf("https://%s.%s", k.vaultName, k.vaultDNSSuffix)
}

// getKeyNameVersion returns the key name and optional version from the key parameter.
func (k *keyvaultCrypto) getKeyNameVersion(key string) (keyVersion, keyName string) {
	idx := strings.IndexRune(key, '/')
	// Can't be on position 0, because the key name must be at least 1 character
	if idx > 0 {
		keyVersion = key[idx+1:]
		keyName = key[:idx]
	} else {
		keyName = key
	}
	return
}
