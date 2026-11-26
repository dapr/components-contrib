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
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"testing"

	"cloud.google.com/go/kms/apiv1/kmspb"
	internals "github.com/dapr/kit/crypto"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKMSRegressionPinnedEncryptionVersion(t *testing.T) {
	for _, operation := range []string{"encrypt", "wrap"} {
		t.Run(operation, func(t *testing.T) {
			client := &fakeClient{}
			k := newTestComponent(client)
			if operation == "encrypt" {
				_, _, err := k.Encrypt(t.Context(), []byte("hello"), AlgorithmSymmetric, "mykey/3", nil, nil)
				require.NoError(t, err)
			} else {
				key, err := jwk.FromRaw(make([]byte, 32))
				require.NoError(t, err)
				_, _, err = k.WrapKey(t.Context(), key, AlgorithmSymmetric, "mykey/3", nil, nil)
				require.NoError(t, err)
			}
			require.NotNil(t, client.encryptRequest)
			assert.Equal(t, testKeyPath+"/cryptoKeyVersions/3", client.encryptRequest.GetName())
		})
	}
}

func TestKMSRegressionRejectsNonceBeforeRPC(t *testing.T) {
	for _, algorithm := range []string{AlgorithmSymmetric, internals.Algorithm_RSA_OAEP_256} {
		for _, operation := range []string{"encrypt", "decrypt", "wrap", "unwrap"} {
			t.Run(algorithm+"/"+operation, func(t *testing.T) {
				client := &fakeClient{}
				k := newTestComponent(client)
				rawKey, err := jwk.FromRaw(make([]byte, 32))
				require.NoError(t, err)
				switch operation {
				case "encrypt":
					_, _, err = k.Encrypt(t.Context(), []byte("hello"), algorithm, "mykey/1", []byte{1}, nil)
				case "decrypt":
					_, err = k.Decrypt(t.Context(), []byte("enc:hello"), algorithm, "mykey/1", []byte{1}, nil, nil)
				case "wrap":
					_, _, err = k.WrapKey(t.Context(), rawKey, algorithm, "mykey/1", []byte{1}, nil)
				case "unwrap":
					_, err = k.UnwrapKey(t.Context(), []byte("enc:hello"), algorithm, "mykey/1", []byte{1}, nil, nil)
				}
				require.ErrorContains(t, err, "nonce is not supported")
				assert.Nil(t, client.encryptRequest)
				assert.Nil(t, client.decryptRequest)
				assert.Nil(t, client.asymDecryptReq)
				assert.Zero(t, client.publicKeyCalls)
			})
		}
	}
}

func TestKMSRegressionRejectsIncompatibleKeyAlgorithm(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	digest := sha256.Sum256([]byte("message"))
	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, digest[:])
	require.NoError(t, err)
	for _, operation := range []string{"encrypt", "decrypt", "wrap", "unwrap", "sign", "verify"} {
		t.Run(operation, func(t *testing.T) {
			client := &fakeClient{publicKeyPEM: publicKeyPEM(t, privateKey.Public()), publicKeyAlgorithm: kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_2048_SHA256, returnedSignature: []byte("signature")}
			if operation == "sign" || operation == "verify" {
				client.publicKeyAlgorithm = kmspb.CryptoKeyVersion_RSA_SIGN_PSS_2048_SHA256
			}
			k := newTestComponent(client)
			rawKey, keyErr := jwk.FromRaw(make([]byte, 32))
			require.NoError(t, keyErr)
			var opErr error
			switch operation {
			case "encrypt":
				_, _, opErr = k.Encrypt(t.Context(), []byte("hello"), internals.Algorithm_RSA_OAEP, "mykey/1", nil, nil)
			case "decrypt":
				_, opErr = k.Decrypt(t.Context(), []byte("ciphertext"), internals.Algorithm_RSA_OAEP, "mykey/1", nil, nil, nil)
			case "wrap":
				_, _, opErr = k.WrapKey(t.Context(), rawKey, internals.Algorithm_RSA_OAEP, "mykey/1", nil, nil)
			case "unwrap":
				_, opErr = k.UnwrapKey(t.Context(), []byte("ciphertext"), internals.Algorithm_RSA_OAEP, "mykey/1", nil, nil, nil)
			case "sign":
				_, opErr = k.Sign(t.Context(), digest[:], internals.Algorithm_RS256, "mykey/1")
			case "verify":
				_, opErr = k.Verify(t.Context(), digest[:], signature, internals.Algorithm_RS256, "mykey/1")
			}
			require.ErrorContains(t, opErr, "incompatible")
			assert.Nil(t, client.asymDecryptReq)
			assert.Nil(t, client.asymSignRequest)
		})
	}
}

func TestKMSRegressionRejectsSeparateTags(t *testing.T) {
	for _, algorithm := range []string{AlgorithmSymmetric, internals.Algorithm_RSA_OAEP_256} {
		t.Run(algorithm, func(t *testing.T) {
			client := &fakeClient{}
			k := newTestComponent(client)
			_, err := k.Decrypt(t.Context(), []byte("enc:hello"), algorithm, "mykey/1", nil, []byte{1}, nil)
			require.ErrorContains(t, err, "separate authentication tags are not supported")
			assert.Nil(t, client.decryptRequest)
			assert.Nil(t, client.asymDecryptReq)
			assert.Zero(t, client.publicKeyCalls)
		})
	}
}

func TestKMSRegressionRejectsUnknownAlgorithm(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	client := &fakeClient{publicKeyPEM: publicKeyPEM(t, privateKey.Public()), returnedSignature: []byte("signature")}
	k := newTestComponent(client)
	_, err = k.Sign(t.Context(), make([]byte, 32), internals.Algorithm_RS256, "mykey/1")
	require.ErrorContains(t, err, "unsupported Cloud KMS key algorithm")
	assert.Nil(t, client.asymSignRequest)
}

func TestKMSRegressionRejectsWrongKeyPurpose(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	t.Run("signing key cannot encrypt", func(t *testing.T) {
		client := &fakeClient{publicKeyPEM: publicKeyPEM(t, privateKey.Public()), publicKeyAlgorithm: kmspb.CryptoKeyVersion_RSA_SIGN_PKCS1_2048_SHA256}
		k := newTestComponent(client)
		_, _, opErr := k.Encrypt(t.Context(), []byte("hello"), internals.Algorithm_RSA_OAEP_256, "mykey/1", nil, nil)
		require.ErrorContains(t, opErr, "incompatible")
	})
	t.Run("decryption key cannot verify a signature", func(t *testing.T) {
		digest := sha256.Sum256([]byte("message"))
		signature, signErr := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, digest[:])
		require.NoError(t, signErr)
		client := &fakeClient{publicKeyPEM: publicKeyPEM(t, privateKey.Public()), publicKeyAlgorithm: kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_2048_SHA256}
		k := newTestComponent(client)
		valid, opErr := k.Verify(t.Context(), digest[:], signature, internals.Algorithm_RS256, "mykey/1")
		require.ErrorContains(t, opErr, "incompatible")
		assert.False(t, valid)
	})
}
