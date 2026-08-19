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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/googleapis/gax-go/v2"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	contribCrypto "github.com/dapr/components-contrib/crypto"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	internals "github.com/dapr/kit/crypto"
	"github.com/dapr/kit/logger"
)

const (
	testKeyPath        = "projects/myproject/locations/global/keyRings/myring/cryptoKeys/mykey"
	testKeyVersionPath = testKeyPath + "/cryptoKeyVersions/1"
)

// Cloud KMS client double, recording the last request of each kind.
type fakeClient struct {
	publicKeyPEM      string
	publicKeyCalls    int
	encryptRequest    *kmspb.EncryptRequest
	decryptRequest    *kmspb.DecryptRequest
	asymDecryptReq    *kmspb.AsymmetricDecryptRequest
	asymSignRequest   *kmspb.AsymmetricSignRequest
	returnedSignature []byte
	err               error
}

func (f *fakeClient) GetPublicKey(_ context.Context, req *kmspb.GetPublicKeyRequest, _ ...gax.CallOption) (*kmspb.PublicKey, error) {
	f.publicKeyCalls++
	if f.err != nil {
		return nil, f.err
	}
	return &kmspb.PublicKey{Name: req.GetName(), Pem: f.publicKeyPEM}, nil
}

func (f *fakeClient) Encrypt(_ context.Context, req *kmspb.EncryptRequest, _ ...gax.CallOption) (*kmspb.EncryptResponse, error) {
	f.encryptRequest = req
	if f.err != nil {
		return nil, f.err
	}
	return &kmspb.EncryptResponse{Ciphertext: append([]byte("enc:"), req.GetPlaintext()...)}, nil
}

func (f *fakeClient) Decrypt(_ context.Context, req *kmspb.DecryptRequest, _ ...gax.CallOption) (*kmspb.DecryptResponse, error) {
	f.decryptRequest = req
	if f.err != nil {
		return nil, f.err
	}
	return &kmspb.DecryptResponse{Plaintext: req.GetCiphertext()[len("enc:"):]}, nil
}

func (f *fakeClient) AsymmetricDecrypt(_ context.Context, req *kmspb.AsymmetricDecryptRequest, _ ...gax.CallOption) (*kmspb.AsymmetricDecryptResponse, error) {
	f.asymDecryptReq = req
	if f.err != nil {
		return nil, f.err
	}
	return &kmspb.AsymmetricDecryptResponse{Plaintext: []byte("decrypted")}, nil
}

func (f *fakeClient) AsymmetricSign(_ context.Context, req *kmspb.AsymmetricSignRequest, _ ...gax.CallOption) (*kmspb.AsymmetricSignResponse, error) {
	f.asymSignRequest = req
	if f.err != nil {
		return nil, f.err
	}
	return &kmspb.AsymmetricSignResponse{Signature: f.returnedSignature}, nil
}

func (f *fakeClient) Close() error { return nil }

func newTestComponent(client kmsClient) *kmsCrypto {
	k := &kmsCrypto{
		logger: logger.NewLogger("test"),
		client: client,
		md: kmsMetadata{
			ProjectID:      "myproject",
			Location:       "global",
			KeyRing:        "myring",
			RequestTimeout: 10 * time.Second,
		},
	}
	k.keyCache = contribCrypto.NewPubKeyCache(k.getKeyCacheFn)
	return k
}

func publicKeyPEM(t *testing.T, pub any) string {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(pub)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
}

func TestMetadata(t *testing.T) {
	newMetadata := func(props map[string]string) contribCrypto.Metadata {
		return contribCrypto.Metadata{Base: contribMetadata.Base{Properties: props}}
	}

	t.Run("valid", func(t *testing.T) {
		md := &kmsMetadata{}
		require.NoError(t, md.InitWithMetadata(newMetadata(map[string]string{
			"projectID": "myproject",
			"location":  "us-east1",
			"keyRing":   "myring",
		})))
		assert.Equal(t, "myproject", md.ProjectID)
		assert.Equal(t, defaultRequestTimeout, md.RequestTimeout)
	})

	t.Run("project ID from the service account alias", func(t *testing.T) {
		md := &kmsMetadata{}
		require.NoError(t, md.InitWithMetadata(newMetadata(map[string]string{
			"project_id":     "myproject",
			"location":       "global",
			"keyRing":        "myring",
			"requestTimeout": "5s",
		})))
		assert.Equal(t, "myproject", md.ProjectID)
		assert.Equal(t, 5*time.Second, md.RequestTimeout)
	})

	t.Run("missing required properties", func(t *testing.T) {
		for _, missing := range []string{"projectID", "location", "keyRing"} {
			props := map[string]string{"projectID": "myproject", "location": "global", "keyRing": "myring"}
			delete(props, missing)

			md := &kmsMetadata{}
			err := md.InitWithMetadata(newMetadata(props))
			require.Error(t, err)
			assert.ErrorContains(t, err, missing)
		}
	})
}

func TestKeyID(t *testing.T) {
	md := kmsMetadata{ProjectID: "myproject", Location: "global", KeyRing: "myring"}

	assert.Equal(t, testKeyPath, md.cryptoKeyPath(newKeyID("mykey")))
	assert.False(t, newKeyID("mykey").Cacheable())

	versionPath, err := md.cryptoKeyVersionPath(newKeyID("mykey/1"))
	require.NoError(t, err)
	assert.Equal(t, testKeyVersionPath, versionPath)
	assert.True(t, newKeyID("mykey/1").Cacheable())

	_, err = md.cryptoKeyVersionPath(newKeyID("mykey"))
	require.Error(t, err)
	assert.ErrorContains(t, err, "does not include a version")
}

func TestGetKey(t *testing.T) {
	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	client := &fakeClient{publicKeyPEM: publicKeyPEM(t, privKey.Public())}
	k := newTestComponent(client)

	pubKey, err := k.GetKey(t.Context(), "mykey/1")
	require.NoError(t, err)
	assert.Equal(t, jwa.EC, pubKey.KeyType())

	// Keys pinned to a version are immutable, so they are only fetched once
	_, err = k.GetKey(t.Context(), "mykey/1")
	require.NoError(t, err)
	assert.Equal(t, 1, client.publicKeyCalls)

	// Asymmetric operations need a key version
	_, err = k.GetKey(t.Context(), "mykey")
	require.Error(t, err)
	assert.ErrorContains(t, err, "does not include a version")
}

func TestSymmetricEncryptDecrypt(t *testing.T) {
	client := &fakeClient{}
	k := newTestComponent(client)

	ciphertext, tag, err := k.Encrypt(t.Context(), []byte("hello"), AlgorithmSymmetric, "mykey", nil, []byte("aad"))
	require.NoError(t, err)
	assert.Nil(t, tag, "Cloud KMS includes the authentication tag in the ciphertext")
	// Symmetric operations address the crypto key, not one of its versions
	assert.Equal(t, testKeyPath, client.encryptRequest.GetName())
	assert.Equal(t, []byte("aad"), client.encryptRequest.GetAdditionalAuthenticatedData())

	plaintext, err := k.Decrypt(t.Context(), ciphertext, AlgorithmSymmetric, "mykey", nil, nil, []byte("aad"))
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), plaintext)
	assert.Equal(t, testKeyPath, client.decryptRequest.GetName())

	// The key version is embedded in the ciphertext, so a version in the key name is not used
	_, err = k.Decrypt(t.Context(), ciphertext, AlgorithmSymmetric, "mykey/1", nil, nil, []byte("aad"))
	require.NoError(t, err)
	assert.Equal(t, testKeyPath, client.decryptRequest.GetName())

	// Cloud KMS generates the nonce itself, so accepting one would be misleading
	_, _, err = k.Encrypt(t.Context(), []byte("hello"), AlgorithmSymmetric, "mykey", []byte("nonce"), nil)
	require.Error(t, err)
	assert.ErrorContains(t, err, "nonce is not supported")
}

func TestAsymmetricEncryptDecrypt(t *testing.T) {
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	client := &fakeClient{publicKeyPEM: publicKeyPEM(t, privKey.Public())}
	k := newTestComponent(client)

	// Encryption happens locally, with the public key retrieved from Cloud KMS
	ciphertext, tag, err := k.Encrypt(t.Context(), []byte("hello"), internals.Algorithm_RSA_OAEP_256, "mykey/1", nil, nil)
	require.NoError(t, err)
	assert.Nil(t, tag)
	assert.Nil(t, client.encryptRequest, "asymmetric encryption must not call the Cloud KMS encrypt API")

	decrypted, err := rsa.DecryptOAEP(sha256.New(), rand.Reader, privKey, ciphertext, nil)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), decrypted)

	// Decryption happens in Cloud KMS, addressing a specific key version
	plaintext, err := k.Decrypt(t.Context(), ciphertext, internals.Algorithm_RSA_OAEP_256, "mykey/1", nil, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, []byte("decrypted"), plaintext)
	assert.Equal(t, testKeyVersionPath, client.asymDecryptReq.GetName())

	// Cloud KMS does not accept an OAEP label, so data encrypted with one could never be decrypted
	_, _, err = k.Encrypt(t.Context(), []byte("hello"), internals.Algorithm_RSA_OAEP_256, "mykey/1", nil, []byte("aad"))
	require.Error(t, err)
	assert.ErrorContains(t, err, "associated data is not supported")

	_, err = k.Decrypt(t.Context(), ciphertext, internals.Algorithm_RSA_OAEP_256, "mykey", nil, nil, nil)
	require.Error(t, err)
	assert.ErrorContains(t, err, "does not include a version")
}

func TestUnsupportedAlgorithm(t *testing.T) {
	k := newTestComponent(&fakeClient{})

	_, _, err := k.Encrypt(t.Context(), []byte("hello"), "A256GCM", "mykey", nil, nil)
	require.Error(t, err)
	assert.ErrorContains(t, err, "invalid algorithm")

	_, err = k.Decrypt(t.Context(), []byte("hello"), "A256GCM", "mykey", nil, nil, nil)
	require.Error(t, err)
	assert.ErrorContains(t, err, "invalid algorithm")

	_, err = k.Sign(t.Context(), make([]byte, 32), "HS256", "mykey/1")
	require.Error(t, err)
	assert.ErrorContains(t, err, "invalid algorithm")
}

func TestWrapUnwrapKey(t *testing.T) {
	client := &fakeClient{}
	k := newTestComponent(client)

	rawKey := []byte("0123456789abcdef0123456789abcdef")
	symmetricKey, err := jwk.FromRaw(rawKey)
	require.NoError(t, err)

	wrapped, tag, err := k.WrapKey(t.Context(), symmetricKey, AlgorithmSymmetric, "mykey", nil, nil)
	require.NoError(t, err)
	assert.Nil(t, tag)
	assert.Equal(t, rawKey, client.encryptRequest.GetPlaintext())

	unwrapped, err := k.UnwrapKey(t.Context(), wrapped, AlgorithmSymmetric, "mykey", nil, nil, nil)
	require.NoError(t, err)
	unwrappedRaw := []byte{}
	require.NoError(t, unwrapped.Raw(&unwrappedRaw))
	assert.Equal(t, rawKey, unwrappedRaw)

	// Asymmetric keys cannot be reconstructed unambiguously after unwrapping
	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	asymmetricKey, err := jwk.FromRaw(privKey)
	require.NoError(t, err)
	_, _, err = k.WrapKey(t.Context(), asymmetricKey, AlgorithmSymmetric, "mykey", nil, nil)
	require.Error(t, err)
	assert.ErrorContains(t, err, "cannot wrap asymmetric keys")
}

func TestSign(t *testing.T) {
	client := &fakeClient{returnedSignature: []byte("signature")}
	k := newTestComponent(client)

	t.Run("digest is sent in the field matching the algorithm", func(t *testing.T) {
		digest := make([]byte, 32)
		signature, err := k.Sign(t.Context(), digest, internals.Algorithm_ES256, "mykey/1")
		require.NoError(t, err)
		assert.Equal(t, []byte("signature"), signature)
		assert.Equal(t, testKeyVersionPath, client.asymSignRequest.GetName())
		assert.Equal(t, digest, client.asymSignRequest.GetDigest().GetSha256())

		_, err = k.Sign(t.Context(), make([]byte, 48), internals.Algorithm_ES384, "mykey/1")
		require.NoError(t, err)
		assert.Equal(t, 48, len(client.asymSignRequest.GetDigest().GetSha384()))

		_, err = k.Sign(t.Context(), make([]byte, 64), internals.Algorithm_PS512, "mykey/1")
		require.NoError(t, err)
		assert.Equal(t, 64, len(client.asymSignRequest.GetDigest().GetSha512()))
	})

	t.Run("Ed25519 signs the message rather than a digest", func(t *testing.T) {
		_, err := k.Sign(t.Context(), []byte("a message of any length"), internals.Algorithm_EdDSA, "mykey/1")
		require.NoError(t, err)
		assert.Equal(t, []byte("a message of any length"), client.asymSignRequest.GetData())
		assert.Nil(t, client.asymSignRequest.GetDigest())
	})

	t.Run("digest length is validated", func(t *testing.T) {
		_, err := k.Sign(t.Context(), make([]byte, 20), internals.Algorithm_ES256, "mykey/1")
		require.Error(t, err)
		assert.ErrorContains(t, err, "must be 32 bytes long")
	})

	t.Run("key version is required", func(t *testing.T) {
		_, err := k.Sign(t.Context(), make([]byte, 32), internals.Algorithm_ES256, "mykey")
		require.Error(t, err)
		assert.ErrorContains(t, err, "does not include a version")
	})

	t.Run("errors from Cloud KMS are returned", func(t *testing.T) {
		failing := newTestComponent(&fakeClient{err: errors.New("permission denied")})
		_, err := failing.Sign(t.Context(), make([]byte, 32), internals.Algorithm_ES256, "mykey/1")
		require.Error(t, err)
		assert.ErrorContains(t, err, "permission denied")
	})
}

func TestVerify(t *testing.T) {
	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	k := newTestComponent(&fakeClient{publicKeyPEM: publicKeyPEM(t, privKey.Public())})

	digest := sha256.Sum256([]byte("hello"))
	signature, err := ecdsa.SignASN1(rand.Reader, privKey, digest[:])
	require.NoError(t, err)

	valid, err := k.Verify(t.Context(), digest[:], signature, internals.Algorithm_ES256, "mykey/1")
	require.NoError(t, err)
	assert.True(t, valid)

	otherDigest := sha256.Sum256([]byte("tampered"))
	valid, err = k.Verify(t.Context(), otherDigest[:], signature, internals.Algorithm_ES256, "mykey/1")
	require.NoError(t, err)
	assert.False(t, valid)
}

func TestSupportedAlgorithms(t *testing.T) {
	k := newTestComponent(&fakeClient{})
	assert.Contains(t, k.SupportedEncryptionAlgorithms(), AlgorithmSymmetric)
	assert.Contains(t, k.SupportedSignatureAlgorithms(), internals.Algorithm_ES256)

	// Every advertised signature algorithm must produce a valid request
	for _, algorithm := range k.SupportedSignatureAlgorithms() {
		digest := make([]byte, 64)
		if algorithm == internals.Algorithm_ES384 {
			digest = make([]byte, 48)
		} else if algorithm == internals.Algorithm_ES256 || algorithm == internals.Algorithm_RS256 || algorithm == internals.Algorithm_PS256 {
			digest = make([]byte, 32)
		}
		_, err := signRequestDigest(digest, algorithm)
		require.NoErrorf(t, err, "algorithm %s", algorithm)
	}
}
