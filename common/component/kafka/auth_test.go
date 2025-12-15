/*
Copyright 2021 The Dapr Authors
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

package kafka

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func getAuthBaseMetadata() map[string]string {
	return map[string]string{
		"consumerGroup": "a", "clientID": "a", "brokers": "a",
		"consumeRetryInterval": "200",
	}
}

func createTestCert() ([]byte, []byte, error) {
	// This helper function with modifications comes from https://github.com/madflojo/testcerts/blob/main/testcerts.go
	// Copyright 2019 Benjamin Cane, MIT License

	// Create a Certificate Authority Cert
	ca := &x509.Certificate{
		Subject: pkix.Name{
			Organization: []string{"Dapr Development Only Organization"},
		},
		SerialNumber:          big.NewInt(123),
		NotAfter:              time.Now().Add(1 * time.Hour),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	// Create a Private Key
	keypair, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, nil, fmt.Errorf("could not generate rsa key - %s", err)
	}

	// Use CA Cert to sign a CSR and create a Public Certificate
	csr := &keypair.PublicKey
	cert, err := x509.CreateCertificate(rand.Reader, ca, ca, csr, keypair)
	if err != nil {
		return nil, nil, fmt.Errorf("could not generate certificate - %s", err)
	}

	// Convert keys into pem.Block
	publiccert := &pem.Block{Type: "CERTIFICATE", Bytes: cert}
	privatekey := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(keypair)}
	return pem.EncodeToMemory(publiccert), pem.EncodeToMemory(privatekey), nil
}

func TestAuth(t *testing.T) {
	k := getKafka()
	publicCert, _, _ := createTestCert()

	t.Run("certificate with auth type 'cert'", func(t *testing.T) {
		m := getAuthBaseMetadata()
		m[caCert] = string(publicCert)
		m[authType] = "certificate"

		meta, err := k.getKafkaMetadata(m)
		require.NoError(t, err)
		require.NotEmpty(t, meta)

		require.False(t, meta.TLSDisable)

		mockConfig := &sarama.Config{}

		tlsconfig := mockConfig.Net.TLS.Config
		require.Nil(t, tlsconfig)

		err = updateTLSConfig(mockConfig, meta)
		require.NoError(t, err)

		require.True(t, mockConfig.Net.TLS.Enable)
		//nolint:staticcheck
		certs := mockConfig.Net.TLS.Config.RootCAs.Subjects()
		require.Len(t, certs, 1)
	})

	t.Run("certificate with auth type 'none'", func(t *testing.T) {
		m := getAuthBaseMetadata()
		m[caCert] = string(publicCert)
		m[authType] = "none"

		meta, err := k.getKafkaMetadata(m)
		require.NoError(t, err)
		require.NotEmpty(t, meta)

		require.False(t, meta.TLSDisable)

		mockConfig := &sarama.Config{}

		tlsconfig := mockConfig.Net.TLS.Config
		require.Nil(t, tlsconfig)

		err = updateTLSConfig(mockConfig, meta)

		require.NoError(t, err)
		require.False(t, mockConfig.Net.TLS.Enable)
		require.Nil(t, mockConfig.Net.TLS.Config)
	})

	t.Run("oidc private key jwt uses flattened audience", func(t *testing.T) {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)
		keyPEM := pem.EncodeToMemory(&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(key),
		})
		certPEM, _, err := createTestCert()
		require.NoError(t, err)

		var receivedAssertion string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.NoError(t, r.ParseForm())
			receivedAssertion = r.FormValue("client_assertion")

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"access_token": "test-token",
				"expires_in":   3600,
			})
		}))
		defer server.Close()

		ts := &OAuthTokenSourcePrivateKeyJWT{
			TokenEndpoint:       oauth2.Endpoint{TokenURL: server.URL},
			ClientID:            "test-client",
			ClientAssertionCert: string(certPEM),
			ClientAssertionKey:  string(keyPEM),
		}

		_, err = ts.Token()
		require.NoError(t, err)
		require.NotEmpty(t, receivedAssertion)

		parts := strings.Split(receivedAssertion, ".")
		require.Len(t, parts, 3, "JWT should have 3 parts")

		decodedPayload, err := base64.RawURLEncoding.DecodeString(parts[1])
		require.NoError(t, err)

		var rawClaims map[string]interface{}
		err = json.Unmarshal(decodedPayload, &rawClaims)
		require.NoError(t, err)

		audValue := rawClaims["aud"]
		require.IsType(t, "", audValue)
	})
}
