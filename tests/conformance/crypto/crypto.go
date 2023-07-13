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
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	contribCrypto "github.com/dapr/components-contrib/crypto"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/tests/conformance/utils"
	"github.com/dapr/kit/config"
)

const (
	// Supported operation "public": if the crypto provider has a public key
	opPublic = "public"
	// Supported operation "symmetric": if the crypto provider has a symmetric key
	opSymmetric = "symmetric"

	// List of required algorithms for private keys
	algsPrivateRequired = "RSA-OAEP PS256 PS384 PS512 RS256 RS384 RS512"
	// List of required algorithms for symmetric keys
	algsSymmetricRequired = "A256CBC A256GCM A256KW"

	// List of all possible symmetric encryption algorithms
	algsEncryptionSymmetric = "A128CBC A192CBC A256CBC A128CBC-NOPAD A192CBC-NOPAD A256CBC-NOPAD A128GCM A192GCM A256GCM A128CBC-HS256 A192CBC-HS384 A256CBC-HS512 C20P XC20P"
	// List of all possible symmetric key wrapping algorithms
	algsKeywrapSymmetric = "A128KW A192KW A256KW A128GCMKW A192GCMKW A256GCMKW C20PKW XC20PKW"
	// List of all possible asymmetric encryption algorithms
	algsEncryptionAsymmetric = "ECDH-ES ECDH-ES+A128KW ECDH-ES+A192KW ECDH-ES+A256KW RSA1_5 RSA-OAEP RSA-OAEP-256 RSA-OAEP-384 RSA-OAEP-512"
	// List of all possible asymmetric key wrapping algorithms
	algsKeywrapAsymmetric = "ECDH-ES ECDH-ES+A128KW ECDH-ES+A192KW ECDH-ES+A256KW RSA1_5 RSA-OAEP RSA-OAEP-256 RSA-OAEP-384 RSA-OAEP-512"
	// List of all possible symmetric signing algorithms
	algsSignSymmetric = "HS256 HS384 HS512"
	// List of all possible asymmetric signing algorithms
	algsSignAsymmetric = "ES256 ES384 ES512 EdDSA PS256 PS384 PS512 RS256 RS384 RS512"
)

type testConfigKey struct {
	// "public", "private", or "symmetric"
	KeyType string `mapstructure:"type"`
	// Algorithm identifiers constant (e.g. "A256CBC")
	Algorithms []string `mapstructure:"algorithms"`
	// Name of the key
	Name string `mapstructure:"name"`
}

type TestConfig struct {
	utils.CommonConfig

	Keys []testConfigKey `mapstructure:"keys"`
}

func NewTestConfig(name string, operations []string, configMap map[string]interface{}) (TestConfig, error) {
	testConfig := TestConfig{
		CommonConfig: utils.CommonConfig{
			ComponentType: "crypto",
			ComponentName: name,
			Operations:    utils.NewStringSet(operations...),
		},
	}

	err := config.Decode(configMap, &testConfig)
	if err != nil {
		return testConfig, err
	}

	return testConfig, nil
}

func ConformanceTests(t *testing.T, props map[string]string, component contribCrypto.SubtleCrypto, config TestConfig) {
	// Parse all keys and algorithms, then ensure the required ones are present
	keys := newKeybagFromConfig(config)
	for _, alg := range strings.Split(algsPrivateRequired, " ") {
		require.Greaterf(t, len(keys.private[alg]), 0, "could not find a private key for algorithm '%s' in configuration, which is required", alg)
	}
	if config.HasOperation(opSymmetric) {
		for _, alg := range strings.Split(algsSymmetricRequired, " ") {
			require.Greaterf(t, len(keys.symmetric[alg]), 0, "could not find a symmetric key for algorithm '%s' in configuration, which is required", alg)
		}
	}
	if config.HasOperation(opPublic) {
		// Require at least one public key
		found := false
		for _, v := range keys.public {
			found = len(v) > 0
			if found {
				break
			}
		}
		require.True(t, found, "could not find any public key in configuration; at least one is required")
	}

	// Init
	t.Run("Init", func(t *testing.T) {
		err := component.Init(context.Background(), contribCrypto.Metadata{Base: metadata.Base{
			Properties: props,
		}})
		require.NoError(t, err, "expected no error on initializing store")
	})

	// Don't run more tests if init failed
	if t.Failed() {
		t.Fatal("Init test failed, stopping further tests")
	}

	// Test SupportedEncryptionAlgorithms and SupportedSignatureAlgorithms.
	// We will just check that the algorithms defined in the tests config are indeed included in the list returned by the component.
	// This test is not perfect as the list of algorithms defined in the config isn't a complete one, but we'll consider this as "sampling".
	t.Run("supported algorithms methods", func(t *testing.T) {
		encAlgs := component.SupportedEncryptionAlgorithms()
		sigAlgs := component.SupportedSignatureAlgorithms()

		algsInList := func(keys keylist, list string, searchInSlice []string) func(t *testing.T) {
			return func(t *testing.T) {
				keys.testForAllAlgorithmsInList(t, list, func(algorithm, _ string) func(t *testing.T) {
					return func(t *testing.T) {
						require.True(t, slices.Contains(searchInSlice, algorithm))
					}
				})
			}
		}

		t.Run("symmetric encryption", algsInList(keys.symmetric, algsEncryptionSymmetric, encAlgs))
		t.Run("symmetric key wrapping", algsInList(keys.symmetric, algsKeywrapSymmetric, encAlgs))
		t.Run("asymmetric encryption with public keys", algsInList(keys.public, algsEncryptionAsymmetric, encAlgs))
		t.Run("asymmetric key wrapping with public keys", algsInList(keys.public, algsKeywrapAsymmetric, encAlgs))
		t.Run("asymmetric encryption with private keys", algsInList(keys.private, algsEncryptionAsymmetric, encAlgs))
		t.Run("asymmetric key wrapping with private keys", algsInList(keys.private, algsKeywrapAsymmetric, encAlgs))
		t.Run("symmetric signature", algsInList(keys.symmetric, algsSignSymmetric, sigAlgs))
		t.Run("asymmetric signature", algsInList(keys.private, algsSignAsymmetric, sigAlgs))
	})

	t.Run("GetKey method", func(t *testing.T) {
		if config.HasOperation(opPublic) {
			t.Run("Get public keys", func(t *testing.T) {
				keys.public.testForAllAlgorithms(t, func(algorithm, keyName string) func(t *testing.T) {
					return func(t *testing.T) {
						ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
						defer cancel()

						key, err := component.GetKey(ctx, keyName)
						require.NoError(t, err)
						assert.NotNil(t, key)
						requireKeyPublic(t, key)
					}
				})
			})
		}

		t.Run("Get public part from private keys", func(t *testing.T) {
			keys.private.testForAllAlgorithms(t, func(algorithm, keyName string) func(t *testing.T) {
				return func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()

					key, err := component.GetKey(ctx, keyName)
					require.NoError(t, err)
					assert.NotNil(t, key)
					requireKeyPublic(t, key)

					// Test how keys are marshaled as JSON
					// We first marshal as JSON and then unmarshal into a POJO to verify that the required keys (the public part of RSA keys) are present
					// For now we test this with RSA only for simplicity
					if algorithm == "RSA-OAEP" {
						j, err := json.Marshal(key)
						require.NoError(t, err)
						require.NotEmpty(t, j)

						keyDict := map[string]any{}
						err = json.Unmarshal(j, &keyDict)
						require.NoError(t, err)

						assert.NotEmptyf(t, keyDict["e"], "missing 'e' property in dictionary: %#v", keyDict)
						assert.NotEmptyf(t, keyDict["n"], "missing 'n' property in dictionary: %#v", keyDict)
						assert.Equal(t, "RSA", keyDict["kty"], "invalid 'kty' property in dictionary: %#v", keyDict)
					}
				}
			})
		})

		t.Run("Cannot get symmetric keys", func(t *testing.T) {
			keys.symmetric.testForAllAlgorithms(t, func(algorithm, keyName string) func(t *testing.T) {
				return func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()

					key, err := component.GetKey(ctx, keyName)
					require.Error(t, err)
					assert.Nil(t, key)
				}
			})
		})
	})

	// Used to test symmetric and asymmetric ciphers for encryption
	testEncryption := func(algorithm string, keyName string, nonce []byte) func(t *testing.T) {
		return func(t *testing.T) {
			// Note: if you change this, make sure it's not a multiple of 16 in length
			const message = "Quel ramo del lago di Como"
			require.False(t, (len(message)%16) == 0, "message must have a length that's not a multiple of 16")

			// Encrypt the message
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			ciphertext, tag, err := component.Encrypt(ctx, []byte(message), algorithm, keyName, nonce, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, ciphertext)
			if hasTag(algorithm) {
				assert.NotEmpty(t, tag)
			} else {
				assert.Nil(t, tag)
			}

			// Decrypt the message
			ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			plaintext, err := component.Decrypt(ctx, ciphertext, algorithm, keyName, nonce, tag, nil)
			require.NoError(t, err)
			assert.Equal(t, message, string(plaintext))

			// Invalid key
			ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, err = component.Decrypt(ctx, ciphertext, algorithm, "foo", nonce, tag, nil)
			require.Error(t, err)

			// Tag mismatch
			if hasTag(algorithm) {
				ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				badTag := randomBytes(t, 16)
				_, err = component.Decrypt(ctx, ciphertext, algorithm, keyName, nonce, badTag, nil)
				require.Error(t, err)
			}
		}
	}

	if config.HasOperation(opSymmetric) {
		t.Run("Symmetric encryption", func(t *testing.T) {
			keys.symmetric.testForAllAlgorithmsInList(t, algsEncryptionSymmetric, func(algorithm, keyName string) func(t *testing.T) {
				nonce := randomBytes(t, nonceSizeForAlgorithm(algorithm))
				return testEncryption(algorithm, keyName, nonce)
			})
		})
	}

	t.Run("Asymmetric encryption with private key", func(t *testing.T) {
		keys.private.testForAllAlgorithmsInList(t, algsEncryptionAsymmetric, func(algorithm, keyName string) func(t *testing.T) {
			return testEncryption(algorithm, keyName, nil)
		})
	})

	if config.HasOperation(opPublic) {
		t.Run("Asymmetric encryption with public key", func(t *testing.T) {
			keys.public.testForAllAlgorithmsInList(t, algsEncryptionAsymmetric, func(algorithm, keyName string) func(t *testing.T) {
				return func(t *testing.T) {
					// Note: if you change this, make sure it's not a multiple of 16 in length
					const message = "Quel ramo del lago di Como"
					require.False(t, (len(message)%16) == 0, "message must have a length that's not a multiple of 16")

					// Encrypt the message
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					ciphertext, tag, err := component.Encrypt(ctx, []byte(message), algorithm, keyName, nil, nil)
					require.NoError(t, err)
					assert.NotEmpty(t, ciphertext)
					if hasTag(algorithm) {
						assert.NotEmpty(t, tag)
					} else {
						assert.Nil(t, tag)
					}
				}
			})
		})
	}

	// Used to test symmetric and asymmetric ciphers for key wrapping
	testKeyWrap := func(algorithm string, keyName string, nonce []byte) func(t *testing.T) {
		return func(t *testing.T) {
			// Key to wrap, random 256-bit
			rawKey := randomBytes(t, 32)
			rawKeyObj, err := jwk.FromRaw(rawKey)
			require.NoError(t, err, "failed to generate key to wrap")

			// Wrap the key
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			wrapped, tag, err := component.WrapKey(ctx, rawKeyObj, algorithm, keyName, nonce, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, wrapped)
			if hasTag(algorithm) {
				assert.NotEmpty(t, tag)
			} else {
				assert.Nil(t, tag)
			}

			// Unwrap the key
			ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			unwrappedObj, err := component.UnwrapKey(ctx, wrapped, algorithm, keyName, nonce, tag, nil)
			require.NoError(t, err)
			var unwrapped []byte
			err = unwrappedObj.Raw(&unwrapped)
			require.NoError(t, err)
			assert.True(t, bytes.Equal(rawKey, unwrapped))

			// Invalid key
			ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, err = component.UnwrapKey(ctx, wrapped, algorithm, "foo", nonce, tag, nil)
			require.Error(t, err)

			// Tag mismatch
			if hasTag(algorithm) {
				ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				badTag := randomBytes(t, 16)
				_, err = component.UnwrapKey(ctx, wrapped, algorithm, keyName, nonce, badTag, nil)
				require.Error(t, err)
			}
		}
	}

	if config.HasOperation(opSymmetric) {
		t.Run("Symmetric key wrap", func(t *testing.T) {
			keys.symmetric.testForAllAlgorithmsInList(t, algsKeywrapSymmetric, func(algorithm, keyName string) func(t *testing.T) {
				nonce := randomBytes(t, nonceSizeForAlgorithm(algorithm))
				return testKeyWrap(algorithm, keyName, nonce)
			})
		})
	}

	t.Run("Asymmetric key wrap with private key", func(t *testing.T) {
		keys.private.testForAllAlgorithmsInList(t, algsKeywrapAsymmetric, func(algorithm, keyName string) func(t *testing.T) {
			return testKeyWrap(algorithm, keyName, nil)
		})
	})

	if config.HasOperation(opPublic) {
		t.Run("Asymmetric key wrap with public key", func(t *testing.T) {
			keys.public.testForAllAlgorithmsInList(t, algsKeywrapAsymmetric, func(algorithm, keyName string) func(t *testing.T) {
				return func(t *testing.T) {
					// Key to wrap, random 256-bit
					rawKey := randomBytes(t, 32)
					rawKeyObj, err := jwk.FromRaw(rawKey)
					require.NoError(t, err, "failed to generate key to wrap")

					// Wrap the key
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					wrapped, tag, err := component.WrapKey(ctx, rawKeyObj, algorithm, keyName, nil, nil)
					require.NoError(t, err)
					assert.NotEmpty(t, wrapped)
					if hasTag(algorithm) {
						assert.NotEmpty(t, tag)
					} else {
						assert.Nil(t, tag)
					}
				}
			})
		})
	}

	// Used to test symmetric and asymmetric ciphers for signing
	testSign := func(algorithm string, keyName string) func(t *testing.T) {
		const message = "Qui dove il mare luccica, e tira forte il vento, su una vecchia terrazza, davanti al golfo di Surriento"

		// Calculate the digest of the message
		digest := hashMessageForSigning([]byte(message), algorithm)

		return func(t *testing.T) {
			// Sign the message
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			signature, err := component.Sign(ctx, digest, algorithm, keyName)
			require.NoError(t, err)
			assert.NotEmpty(t, signature)

			// Verify the signature
			valid, err := component.Verify(ctx, digest, signature, algorithm, keyName)
			require.NoError(t, err)
			assert.True(t, valid)

			// Change the digest to make the verification fail
			digest[0]++ // Note that this may overflow, and that's ok
			valid, err = component.Verify(ctx, digest, signature, algorithm, keyName)
			require.NoError(t, err)
			assert.False(t, valid)
		}
	}

	t.Run("Sign and verify with private key", func(t *testing.T) {
		keys.private.testForAllAlgorithmsInList(t, algsSignAsymmetric, testSign)
	})

	if config.HasOperation(opSymmetric) {
		t.Run("Sign and verify with symmetric key", func(t *testing.T) {
			keys.symmetric.testForAllAlgorithmsInList(t, algsSignSymmetric, testSign)
		})
	}
}
