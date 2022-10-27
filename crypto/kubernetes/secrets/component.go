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

package secrets

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	daprcrypto "github.com/dapr/components-contrib/crypto"
	kubeclient "github.com/dapr/components-contrib/internal/authentication/kubernetes"
	internals "github.com/dapr/components-contrib/internal/crypto"
	"github.com/dapr/kit/logger"
)

const (
	requestTimeout              = 30 * time.Second
	metadataKeyDefaultNamespace = "defaultNamespace"
)

var errNotFound = errors.New("key not found in the Kubernetes secrets")

type kubeSecretsCrypto struct {
	defaultNamespace string
	kubeClient       kubernetes.Interface
	logger           logger.Logger
}

// NewKubeSecretsCrypto returns a new Kubernetes secrets crypto provider.
func NewKubeSecretsCrypto(logger logger.Logger) daprcrypto.SubtleCrypto {
	return &kubeSecretsCrypto{
		logger: logger,
	}
}

// Init creates a Kubernetes secrets crypto provider.
func (k *kubeSecretsCrypto) Init(metadata daprcrypto.Metadata) error {
	if len(metadata.Properties) > 0 {
		if metadata.Properties[metadataKeyDefaultNamespace] != "" {
			k.defaultNamespace = metadata.Properties[metadataKeyDefaultNamespace]
		}
	}

	client, err := kubeclient.GetKubeClient()
	if err != nil {
		return err
	}
	k.kubeClient = client

	return nil
}

// Features returns the features available in this crypto provider.
func (k *kubeSecretsCrypto) Features() []daprcrypto.Feature {
	return []daprcrypto.Feature{} // No Feature supported.
}

// GetKey returns the public part of a key stored in the vault.
// This method returns an error if the key is symmetric.
// The key argument can be in the format "namespace/secretName/key" or "secretName/key" if using the default namespace passed as component metadata.
func (k *kubeSecretsCrypto) GetKey(parentCtx context.Context, key string) (pubKey *daprcrypto.Key, err error) {
	jwkObj, err := k.retrieveKeyFromSecret(parentCtx, key)
	if err != nil {
		return nil, err
	}

	// We can only return public keys
	if jwkObj.KeyType() == jwa.OctetSeq {
		// If the key exists but it's not of the right kind, return a not-found error
		return nil, errNotFound
	}
	pk, err := jwkObj.PublicKey()
	if err != nil {
		return nil, fmt.Errorf("failed to obtain public key: %w", err)
	}
	return daprcrypto.NewKey(pk, nil, nil), nil
}

// Retrieves a key (public or private or symmetric) from a Kubernetes secret.
func (k *kubeSecretsCrypto) retrieveKeyFromSecret(parentCtx context.Context, key string) (jwk.Key, error) {
	keyNamespace, keySecret, keyName, err := k.parseKeyString(key)
	if err != nil {
		return nil, err
	}

	// Retrieve the secret
	ctx, cancel := context.WithTimeout(parentCtx, requestTimeout)
	res, err := k.kubeClient.CoreV1().
		Secrets(keyNamespace).
		Get(ctx, keySecret, metaV1.GetOptions{})
	cancel()
	if err != nil {
		return nil, err
	}
	if res == nil || len(res.Data) == 0 || len(res.Data[keyName]) == 0 {
		return nil, errNotFound
	}

	// Parse the key
	jwkObj, err := internals.ParseKey(res.Data[keyName], string(res.Type))
	if err == nil {
		switch jwkObj.KeyType() {
		case jwa.EC, jwa.RSA, jwa.OKP, jwa.OctetSeq:
			// Nop
		default:
			err = errors.New("invalid key type")
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to parse key from secret: %w", err)
	}

	return jwkObj, nil
}

func (k *kubeSecretsCrypto) Encrypt(parentCtx context.Context, plaintext []byte, algorithm string, keyName string, nonce []byte, associatedData []byte) (ciphertext []byte, tag []byte, err error) {
	// Retrieve the key
	key, err := k.retrieveKeyFromSecret(parentCtx, keyName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to retrieve the key: %w", err)
	}

	// Encrypt the data
	ciphertext, tag, err = internals.Encrypt(plaintext, algorithm, key, nonce, associatedData)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to encrypt data: %w", err)
	}
	return ciphertext, tag, nil
}

func (k *kubeSecretsCrypto) Decrypt(parentCtx context.Context, ciphertext []byte, algorithm string, keyName string, nonce []byte, tag []byte, associatedData []byte) (plaintext []byte, err error) {
	// Retrieve the key
	key, err := k.retrieveKeyFromSecret(parentCtx, keyName)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve the key: %w", err)
	}

	// Decrypt the data
	plaintext, err = internals.Decrypt(ciphertext, algorithm, key, nonce, tag, associatedData)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt data: %w", err)
	}
	return plaintext, nil
}

func (k *kubeSecretsCrypto) WrapKey(parentCtx context.Context, plaintextKey jwk.Key, algorithm string, keyName string, nonce []byte, associatedData []byte) (wrappedKey []byte, tag []byte, err error) {
	// Serialize the plaintextKey
	plaintext, err := internals.SerializeKey(plaintextKey)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot serialize key: %w", err)
	}

	// Retrieve the key encryption key
	kek, err := k.retrieveKeyFromSecret(parentCtx, keyName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to retrieve the key encryption key: %w", err)
	}

	// Encrypt the data
	wrappedKey, tag, err = internals.Encrypt(plaintext, algorithm, kek, nonce, associatedData)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to encrypt data: %w", err)
	}
	return wrappedKey, tag, nil
}

func (k *kubeSecretsCrypto) UnwrapKey(parentCtx context.Context, wrappedKey []byte, algorithm string, keyName string, nonce []byte, tag []byte, associatedData []byte) (plaintextKey jwk.Key, err error) {
	// Retrieve the key encryption key
	kek, err := k.retrieveKeyFromSecret(parentCtx, keyName)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve the key encryption key: %w", err)
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

func (k *kubeSecretsCrypto) Sign(parentCtx context.Context, digest []byte, algorithm string, keyName string) (signature []byte, err error) {
	// Retrieve the key
	key, err := k.retrieveKeyFromSecret(parentCtx, keyName)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve the key: %w", err)
	}

	// Sign the message
	signature, err = internals.SignPrivateKey(digest, algorithm, key)
	if err != nil {
		return nil, fmt.Errorf("failed to sign message: %w", err)
	}
	return signature, nil
}

func (k *kubeSecretsCrypto) Verify(parentCtx context.Context, digest []byte, signature []byte, algorithm string, keyName string) (valid bool, err error) {
	// Retrieve the key
	key, err := k.retrieveKeyFromSecret(parentCtx, keyName)
	if err != nil {
		return false, fmt.Errorf("failed to retrieve the key: %w", err)
	}

	// Verify the signature
	valid, err = internals.VerifyPublicKey(digest, signature, algorithm, key)
	if err != nil {
		return false, fmt.Errorf("failed to validate the signature: %w", err)
	}
	return valid, nil
}

// parseKeyString returns the secret name, key, and optional namespace from the key parameter.
// If the key parameter doesn't contain a namespace, returns the default one.
func (k *kubeSecretsCrypto) parseKeyString(param string) (namespace string, secret string, key string, err error) {
	parts := strings.Split(key, "/")
	switch len(parts) {
	case 3:
		namespace = parts[0]
		secret = parts[1]
		key = parts[2]
	case 2:
		namespace = k.defaultNamespace
		secret = parts[0]
		key = parts[1]
	default:
		err = errors.New("key is not in a valid format: required namespace/secretName/key or secretName/key")
	}

	if namespace == "" {
		err = errors.New("key doesn't have a namespace and the default namespace isn't set")
	}

	return
}
