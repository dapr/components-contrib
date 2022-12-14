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
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"io"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	daprcrypto "github.com/dapr/components-contrib/crypto"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/tests/conformance/utils"
	"github.com/dapr/kit/config"
)

// creating this struct so that it can be expanded later.
type TestConfig struct {
	utils.CommonConfig

	PublicKeyName     string `mapstructure:"publicKeyName"`
	PrivateKeyName    string `mapstructure:"privateKeyName"`
	AltPrivateKeyName string `mapstructure:"altPrivateKeyName"`
	SymmetricKeyName  string `mapstructure:"symmetricKeyName"`
	SymmetricCipher   string `mapstructure:"symmetricCipher"`
}

func NewTestConfig(name string, allOperations bool, operations []string, configMap map[string]interface{}) (TestConfig, error) {
	testConfig := TestConfig{
		CommonConfig: utils.CommonConfig{
			ComponentType: "crypto",
			ComponentName: name,
			AllOperations: allOperations,
			Operations:    utils.NewStringSet(operations...),
		},
		PublicKeyName:     "pubkey",
		PrivateKeyName:    "privkey",
		AltPrivateKeyName: "altprivkey",
		SymmetricKeyName:  "symmetric",
		SymmetricCipher:   "A256GCM",
	}

	err := config.Decode(configMap, &testConfig)
	if err != nil {
		return testConfig, err
	}

	return testConfig, nil
}

func ConformanceTests(t *testing.T, props map[string]string, component daprcrypto.SubtleCrypto, config TestConfig) {
	// Init
	t.Run("Init", func(t *testing.T) {
		err := component.Init(daprcrypto.Metadata{
			Base: metadata.Base{Properties: props},
		})
		require.NoError(t, err, "expected no error on initializing store")
	})

	// Don't run more tests if init failed
	if t.Failed() {
		t.Fatal("Init test failed, stopping further tests")
	}

	t.Run("Get key", func(t *testing.T) {
		t.Run("Get public keys", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			key, err := component.GetKey(ctx, config.PublicKeyName)
			require.NoError(t, err)
			assert.NotNil(t, key)
			requireKeyPublic(t, key)
		})

		t.Run("Get public part from private keys", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			key, err := component.GetKey(ctx, config.PrivateKeyName)
			require.NoError(t, err)
			assert.NotNil(t, key)
			requireKeyPublic(t, key)
		})

		t.Run("Cannot get symmetric keys", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			key, err := component.GetKey(ctx, config.SymmetricKeyName)
			require.Error(t, err)
			assert.Nil(t, key)
		})
	})

	t.Run("Symmetric encryption with "+config.SymmetricCipher, func(t *testing.T) {
		nonce := randomBytes(t, 12)

		const message = "Quel ramo del lago di Como"

		// Encrypt the message
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		ciphertext, tag, err := component.Encrypt(ctx, []byte(message), config.SymmetricCipher, config.SymmetricKeyName, nonce, nil)
		require.NoError(t, err)
		assert.NotEmpty(t, ciphertext)
		assert.NotEmpty(t, tag)

		// Decrypt the message
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		plaintext, err := component.Decrypt(ctx, ciphertext, config.SymmetricCipher, config.SymmetricKeyName, nonce, tag, nil)
		require.NoError(t, err)
		assert.Equal(t, message, string(plaintext))

		// Invalid key
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err = component.Decrypt(ctx, ciphertext, config.SymmetricCipher, config.PrivateKeyName, nonce, tag, nil)
		require.Error(t, err)

		// Tag mismatch
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		badTag := randomBytes(t, 16)
		_, err = component.Decrypt(ctx, ciphertext, config.SymmetricCipher, config.SymmetricKeyName, nonce, badTag, nil)
		require.Error(t, err)
	})
}

func randomBytes(t *testing.T, size int) []byte {
	t.Helper()

	b := make([]byte, size)
	l, err := io.ReadFull(rand.Reader, b)
	require.NoError(t, err)
	require.Equal(t, size, l)
	return b
}

func requireKeyPublic(t *testing.T, key jwk.Key) {
	t.Helper()

	var rawKey any
	err := key.Raw(&rawKey)
	require.NoError(t, err)

	switch rawKey.(type) {
	case ed25519.PublicKey,
		rsa.PublicKey,
		*rsa.PublicKey,
		ecdsa.PublicKey,
		*ecdsa.PublicKey:
		// all good - nop
	default:
		t.Errorf("key is not a public key: %T", rawKey)
	}
}
