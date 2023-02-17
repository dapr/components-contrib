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
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"testing"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/stretchr/testify/require"
)

func TestSymmetricKeys(t *testing.T) {
	rawKey := make([]byte, 16)
	_, rawErr := io.ReadFull(rand.Reader, rawKey)
	require.NoError(t, rawErr)
	rawKeyJSON, _ := json.Marshal(map[string]any{
		"kty": "oct",
		"k":   rawKey,
	})

	testSymmetricKey := func(parse []byte, contentType string) func(t *testing.T) {
		return func(t *testing.T) {
			var (
				key      jwk.Key
				exported []byte
				err      error
			)
			t.Run("parse", func(t *testing.T) {
				key, err = ParseKey(parse, contentType)
				require.NoError(t, err)
				require.Equal(t, jwa.OctetSeq, key.KeyType())

				err = key.Raw(&exported)
				require.NoError(t, err)
				require.Equal(t, rawKey, exported)
			})

			t.Run("serialize", func(t *testing.T) {
				require.NotNil(t, key)
				exported, err = SerializeKey(key)
				require.NoError(t, err)
				require.Equal(t, rawKey, exported)
			})
		}
	}

	t.Run("raw bytes", testSymmetricKey(rawKey, ""))
	t.Run("base64 standard with padding", testSymmetricKey([]byte(base64.StdEncoding.EncodeToString(rawKey)), ""))
	t.Run("base64 standard without padding", testSymmetricKey([]byte(base64.RawStdEncoding.EncodeToString(rawKey)), ""))
	t.Run("base64 url with padding", testSymmetricKey([]byte(base64.URLEncoding.EncodeToString(rawKey)), ""))
	t.Run("base64 url without padding", testSymmetricKey([]byte(base64.RawURLEncoding.EncodeToString(rawKey)), ""))
	t.Run("JSON with content-type", testSymmetricKey(rawKeyJSON, "application/json"))
	t.Run("JSON without content-type", testSymmetricKey(rawKeyJSON, ""))
}

func TestPrivateKeysRSA(t *testing.T) {
	testPrivateKey := func(parse []byte, contentType string) func(t *testing.T) {
		return func(t *testing.T) {
			var (
				key      jwk.Key
				exported []byte
				err      error
			)
			t.Run("parse", func(t *testing.T) {
				key, err = ParseKey(parse, contentType)
				require.NoError(t, err)
				require.Equal(t, jwa.RSA, key.KeyType())
			})

			t.Run("serialize", func(t *testing.T) {
				require.NotNil(t, key)
				exported, err = SerializeKey(key)
				require.NoError(t, err)
				require.NotEmpty(t, exported)
				exportedPEM := pem.EncodeToMemory(&pem.Block{
					Type:  "PRIVATE KEY",
					Bytes: exported,
				})
				require.Equal(t, privateKeyRSAPKCS8, string(exportedPEM))
			})
		}
	}

	t.Run("PEM PKCS#8 with content-type", testPrivateKey([]byte(privateKeyRSAPKCS8), "application/x-pem-file"))
	t.Run("PEM PKCS#8 with content-type 2", testPrivateKey([]byte(privateKeyRSAPKCS8), "application/pkcs8"))
	t.Run("PEM PKCS#8 without content-type", testPrivateKey([]byte(privateKeyRSAPKCS8), ""))
	t.Run("PEM PKCS#1 with content-type", testPrivateKey([]byte(privateKeyRSAPKCS8), "application/x-pem-file"))
	t.Run("PEM PKCS#1 without content-type", testPrivateKey([]byte(privateKeyRSAPKCS8), ""))
	t.Run("JSON with content-type", testPrivateKey([]byte(privateKeyRSAJSON), "application/json"))
	t.Run("JSON without content-type", testPrivateKey([]byte(privateKeyRSAJSON), ""))
}

func TestPublicKeysRSA(t *testing.T) {
	testPublicKey := func(parse []byte, contentType string) func(t *testing.T) {
		return func(t *testing.T) {
			var (
				key      jwk.Key
				exported []byte
				err      error
			)
			t.Run("parse", func(t *testing.T) {
				key, err = ParseKey(parse, contentType)
				require.NoError(t, err)
				require.Equal(t, jwa.RSA, key.KeyType())
			})

			t.Run("serialize", func(t *testing.T) {
				require.NotNil(t, key)
				exported, err = SerializeKey(key)
				require.NoError(t, err)
				require.NotEmpty(t, exported)
				exportedPEM := pem.EncodeToMemory(&pem.Block{
					Type:  "PUBLIC KEY",
					Bytes: exported,
				})
				require.Equal(t, publicKeyRSAPKIX, string(exportedPEM))
			})
		}
	}

	t.Run("PEM PKIX with content-type", testPublicKey([]byte(publicKeyRSAPKIX), "application/x-pem-file"))
	t.Run("PEM PKIX without content-type", testPublicKey([]byte(publicKeyRSAPKIX), ""))
	t.Run("PEM PKCS#1 with content-type", testPublicKey([]byte(publicKeyRSAPKCS1), "application/x-pem-file"))
	t.Run("PEM PKCS#1 without content-type", testPublicKey([]byte(publicKeyRSAPKCS1), ""))
	t.Run("JSON with content-type", testPublicKey([]byte(publicKeyRSAJSON), "application/json"))
	t.Run("JSON without content-type", testPublicKey([]byte(publicKeyRSAJSON), ""))
}

func TestPrivateKeysEd25519(t *testing.T) {
	testPrivateKey := func(parse []byte, contentType string) func(t *testing.T) {
		return func(t *testing.T) {
			var (
				key      jwk.Key
				exported []byte
				err      error
			)
			t.Run("parse", func(t *testing.T) {
				key, err = ParseKey(parse, contentType)
				require.NoError(t, err)
				require.Equal(t, jwa.OKP, key.KeyType())
				crv, ok := key.Get("crv")
				require.True(t, ok)
				require.Equal(t, jwa.Ed25519, crv)
			})

			t.Run("serialize", func(t *testing.T) {
				require.NotNil(t, key)
				exported, err = SerializeKey(key)
				require.NoError(t, err)
				require.NotEmpty(t, exported)
				exportedPEM := pem.EncodeToMemory(&pem.Block{
					Type:  "PRIVATE KEY",
					Bytes: exported,
				})
				require.Equal(t, privateKeyEd25519PKCS8, string(exportedPEM))
			})
		}
	}

	t.Run("PEM PKCS#8 with content-type", testPrivateKey([]byte(privateKeyEd25519PKCS8), "application/x-pem-file"))
	t.Run("PEM PKCS#8 with content-type 2", testPrivateKey([]byte(privateKeyEd25519PKCS8), "application/pkcs8"))
	t.Run("PEM PKCS#8 without content-type", testPrivateKey([]byte(privateKeyEd25519PKCS8), ""))
	t.Run("JSON with content-type", testPrivateKey([]byte(privateKeyEd25519JSON), "application/json"))
	t.Run("JSON without content-type", testPrivateKey([]byte(privateKeyEd25519JSON), ""))
}

func TestPublicKeysEd25519(t *testing.T) {
	testPublicKey := func(parse []byte, contentType string) func(t *testing.T) {
		return func(t *testing.T) {
			var (
				key      jwk.Key
				exported []byte
				err      error
			)
			t.Run("parse", func(t *testing.T) {
				key, err = ParseKey(parse, contentType)
				require.NoError(t, err)
				require.Equal(t, jwa.OKP, key.KeyType())
				crv, ok := key.Get("crv")
				require.True(t, ok)
				require.Equal(t, jwa.Ed25519, crv)
			})

			t.Run("serialize", func(t *testing.T) {
				require.NotNil(t, key)
				exported, err = SerializeKey(key)
				require.NoError(t, err)
				require.NotEmpty(t, exported)
				exportedPEM := pem.EncodeToMemory(&pem.Block{
					Type:  "PUBLIC KEY",
					Bytes: exported,
				})
				require.Equal(t, publicKeyEd25519PKIX, string(exportedPEM))
			})
		}
	}

	t.Run("PEM PKIX with content-type", testPublicKey([]byte(publicKeyEd25519PKIX), "application/x-pem-file"))
	t.Run("PEM PKIX without content-type", testPublicKey([]byte(publicKeyEd25519PKIX), ""))
	t.Run("JSON with content-type", testPublicKey([]byte(publicKeyEd25519JSON), "application/json"))
	t.Run("JSON without content-type", testPublicKey([]byte(publicKeyEd25519JSON), ""))
}

func TestPrivateKeysP256(t *testing.T) {
	testPrivateKey := func(parse []byte, contentType string) func(t *testing.T) {
		return func(t *testing.T) {
			var (
				key      jwk.Key
				exported []byte
				err      error
			)
			t.Run("parse", func(t *testing.T) {
				key, err = ParseKey(parse, contentType)
				require.NoError(t, err)
				require.Equal(t, jwa.EC, key.KeyType())
				crv, ok := key.Get("crv")
				require.True(t, ok)
				require.Equal(t, jwa.P256, crv)
			})

			t.Run("serialize", func(t *testing.T) {
				require.NotNil(t, key)
				exported, err = SerializeKey(key)
				require.NoError(t, err)
				require.NotEmpty(t, exported)
				exportedPEM := pem.EncodeToMemory(&pem.Block{
					Type:  "PRIVATE KEY",
					Bytes: exported,
				})
				require.Equal(t, privateKeyP256PKCS8, string(exportedPEM))
			})
		}
	}

	t.Run("PEM EC with content-type", testPrivateKey([]byte(privateKeyP256EC), "application/x-pem-file"))
	t.Run("PEM EC without content-type", testPrivateKey([]byte(privateKeyP256EC), ""))
	t.Run("PEM PKCS8 with content-type", testPrivateKey([]byte(privateKeyP256PKCS8), "application/x-pem-file"))
	t.Run("PEM PKCS8 without content-type", testPrivateKey([]byte(privateKeyP256PKCS8), ""))
	t.Run("JSON with content-type", testPrivateKey([]byte(privateKeyP256JSON), "application/json"))
	t.Run("JSON without content-type", testPrivateKey([]byte(privateKeyP256JSON), ""))
}

func TestPublicKeysP256(t *testing.T) {
	testPublicKey := func(parse []byte, contentType string) func(t *testing.T) {
		return func(t *testing.T) {
			var (
				key      jwk.Key
				exported []byte
				err      error
			)
			t.Run("parse", func(t *testing.T) {
				key, err = ParseKey(parse, contentType)
				require.NoError(t, err)
				require.Equal(t, jwa.EC, key.KeyType())
				crv, ok := key.Get("crv")
				require.True(t, ok)
				require.Equal(t, jwa.P256, crv)
			})

			t.Run("serialize", func(t *testing.T) {
				require.NotNil(t, key)
				exported, err = SerializeKey(key)
				require.NoError(t, err)
				require.NotEmpty(t, exported)
				exportedPEM := pem.EncodeToMemory(&pem.Block{
					Type:  "PUBLIC KEY",
					Bytes: exported,
				})
				require.Equal(t, publicKeyP256PKIX, string(exportedPEM))
			})
		}
	}

	t.Run("PEM PKIX with content-type", testPublicKey([]byte(publicKeyP256PKIX), "application/x-pem-file"))
	t.Run("PEM PKIX without content-type", testPublicKey([]byte(publicKeyP256PKIX), ""))
	t.Run("JSON with content-type", testPublicKey([]byte(publicKeyP256JSON), "application/json"))
	t.Run("JSON without content-type", testPublicKey([]byte(publicKeyP256JSON), ""))
}
