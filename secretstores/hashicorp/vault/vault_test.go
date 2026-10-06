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

package vault

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	// base64 encoded certificate.
	certificate                    = "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURVakNDQWpvQ0NRRFlZdzdMeXN4VXRUQU5CZ2txaGtpRzl3MEJBUXNGQURCck1Rc3dDUVlEVlFRR0V3SkQKUVRFWk1CY0dBMVVFQ0F3UVFuSnBkR2x6YUNCRGIyeDFiV0pwWVRFU01CQUdBMVVFQnd3SlZtRnVZMjkxZG1WeQpNUk13RVFZRFZRUUtEQXB0YVhOb2NtRmpiM0p3TVJnd0ZnWURWUVFEREE5MllYVnNkSEJ5YjJwbFkzUXVhVzh3CkhoY05NVGt4TVRBeE1UQTBPREV5V2hjTk1qQXhNRE14TVRBME9ERXlXakJyTVFzd0NRWURWUVFHRXdKRFFURVoKTUJjR0ExVUVDQXdRUW5KcGRHbHphQ0JEYjJ4MWJXSnBZVEVTTUJBR0ExVUVCd3dKVm1GdVkyOTFkbVZ5TVJNdwpFUVlEVlFRS0RBcHRhWE5vY21GamIzSndNUmd3RmdZRFZRUUREQTkyWVhWc2RIQnliMnBsWTNRdWFXOHdnZ0VpCk1BMEdDU3FHU0liM0RRRUJBUVVBQTRJQkR3QXdnZ0VLQW9JQkFRQ3JtaitTTmtGUHEvK2FXUFV1MlpFamtSK3AKTm1PeEVNSnZZcGhHNkJvRFAySE9ZbGRzdk9FWkRkbTBpWFlmeFIwZm5rUmtTMWEzSlZiYmhINWJnTElKb0dxcwo5aWpzN2hyQ0Rrdk9uRWxpUEZuc05pQ2NWNDNxNkZYaFMvNFpoNGpOMnlyUkU2UmZiS1BEeUw0a282NkFhSld1CnVkTldKVWpzSFZBSWowZHlnTXFKYm0rT29iSzk5ckUxcDg5Z3RNUStJdzFkWnUvUFF4SjlYOStMeXdxZUxPckQKOWhpNWkxajNFUUp2RXQxSVUzclEwc2E0NU5zZkt4YzEwZjdhTjJuSDQzSnhnMVRiZXNPOWYrcWlyeDBHYmVSYQpyVmNaazNVaFc2cHZmam9XbDBEc0NwNTJwZDBQN05rUmhmak44b2RMN0h3bFVIc1NqemlSYytsTG5YREJBZ01CCkFBRXdEUVlKS29aSWh2Y05BUUVMQlFBRGdnRUJBSVdKdmRPZ01PUnQxWk53SENkNTNieTlkMlBkcW5tWHFZZ20KNDZHK2Fvb1dSeTJKMEMwS3ZOVGZGbEJFOUlydzNXUTVNMnpqY25qSUp5bzNLRUM5TDdPMnQ1WC9LTGVDck5ZVgpIc1d4cU5BTVBGY2VBa09HT0I1TThGVllkdjJTaVV2UDJjMEZQSzc2WFVzcVNkdnRsWGFkTk5ENzE3T0NTNm0yCnBIVjh1NWJNd1VmR2NCVFpEV2o4bjIzRVdHaXdnYkJkdDc3Z3h3YWc5NTROZkM2Ny9nSUc5ZlRrTTQ4aVJCUzEKc0NGYVBjMkFIT3hiMSs0ajVCMVY2Z29iZDZYWkFvbHdNaTNHUUtkbEM1NXZNeTNwK09WbDNNbEc3RWNTVUpMdApwZ2ZKaWw3L3dTWWhpUnhJU3hrYkk5cWhvNEwzZm5PZVB3clFVd2FzU1ZiL1lxbHZ2WHM9Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K"
	expectedTok                    = "myRootToken"
	expectedTokenMountFileContents = "Hey! TokenMountFile contents here!"
)

func createTempFileWithContent(t *testing.T, contents string) (fileName string, cleanUpFunc func()) {
	dir := os.TempDir()
	f, err := os.CreateTemp(dir, "vault-token")
	require.NoError(t, err)
	fileName = f.Name()
	cleanUpFunc = func() {
		os.Remove(fileName)
	}

	_, err = f.WriteString(contents)
	require.NoError(t, err)

	return fileName, cleanUpFunc
}

func createTokenMountPathFile(t *testing.T) (fileName string, cleanUpFunc func()) {
	return createTempFileWithContent(t, expectedTokenMountFileContents)
}

func TestReadVaultToken(t *testing.T) {
	tokenString := "This-IS-TheRootToken"
	tmpFileName, cleanUpFunc := createTempFileWithContent(t, tokenString)
	defer cleanUpFunc()

	t.Run("read correct token", func(t *testing.T) {
		v := vaultSecretStore{
			vaultTokenMountPath: tmpFileName,
		}

		err := v.initVaultToken()
		require.NoError(t, err)
		assert.Equal(t, tokenString, v.vaultToken)
	})

	t.Run("read incorrect token", func(t *testing.T) {
		v := vaultSecretStore{
			vaultTokenMountPath: tmpFileName,
		}

		err := v.initVaultToken()
		require.NoError(t, err)
		assert.NotEqual(t, "ThisIs-NOT-TheRootToken", v.vaultToken)
	})

	t.Run("read token from vaultToken", func(t *testing.T) {
		v := vaultSecretStore{
			vaultToken: expectedTok,
		}

		err := v.initVaultToken()

		require.NoError(t, err)
		assert.Equal(t, expectedTok, v.vaultToken)
	})
}

func TestVaultTLSConfig(t *testing.T) {
	t.Run("with tls configuration", func(t *testing.T) {
		certBytes := getCertificate()
		properties := map[string]string{
			"caCert":        string(certBytes),
			"skipVerify":    "false",
			"tlsServerName": "vaultproject.io",
		}

		m := secretstores.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		meta := VaultMetadata{}
		err := kitmd.DecodeMetadata(m.Properties, &meta)
		require.NoError(t, err)

		tlsConf := metadataToTLSConfig(&meta)
		assert.Equal(t, properties["caCert"], tlsConf.CACert)
		assert.False(t, tlsConf.Insecure)
		assert.Equal(t, properties["tlsServerName"], tlsConf.TLSServerName)
	})

	t.Run("skipVerify true sets Insecure", func(t *testing.T) {
		meta := VaultMetadata{SkipVerify: "true"}
		tlsConf := metadataToTLSConfig(&meta)
		assert.True(t, tlsConf.Insecure)
	})

	t.Run("caPem takes precedence over caPath and caCert", func(t *testing.T) {
		meta := VaultMetadata{
			CaPem:  "pem-contents",
			CaPath: "/some/path",
			CaCert: "/some/cert",
		}

		tlsConf := metadataToTLSConfig(&meta)
		assert.Equal(t, []byte("pem-contents"), tlsConf.CACertBytes)
		assert.Empty(t, tlsConf.CACert)
		assert.Empty(t, tlsConf.CAPath)
	})

	t.Run("caPath takes precedence over caCert when caPem is not set", func(t *testing.T) {
		meta := VaultMetadata{
			CaPath: "/some/path",
			CaCert: "/some/cert",
		}

		tlsConf := metadataToTLSConfig(&meta)
		assert.Equal(t, "/some/path", tlsConf.CAPath)
		assert.Empty(t, tlsConf.CACert)
		assert.Empty(t, tlsConf.CACertBytes)
	})
}

func TestVaultEnginePath(t *testing.T) {
	t.Run("without engine path config", func(t *testing.T) {
		v := vaultSecretStore{
			logger: logger.NewLogger("test"),
		}

		err := v.Init(t.Context(), secretstores.Metadata{Base: metadata.Base{Properties: map[string]string{componentVaultToken: expectedTok, "skipVerify": "true"}}})
		require.NoError(t, err)
		assert.Equal(t, defaultVaultEnginePath, v.vaultEnginePath)
	})

	t.Run("with engine path config", func(t *testing.T) {
		v := vaultSecretStore{
			logger: logger.NewLogger("test"),
		}

		err := v.Init(t.Context(), secretstores.Metadata{Base: metadata.Base{Properties: map[string]string{componentVaultToken: expectedTok, "skipVerify": "true", vaultEnginePath: "kv"}}})
		require.NoError(t, err)
		assert.Equal(t, "kv", v.vaultEnginePath)
	})
}

func TestVaultTokenPrefix(t *testing.T) {
	expectedTokMountPath, cleanUpFunc := createTokenMountPathFile(t)
	defer cleanUpFunc()

	t.Run("default value of vaultKVUsePrefix is true to emulate previous behaviour", func(t *testing.T) {
		properties := map[string]string{
			componentVaultToken: expectedTok,
		}

		m := secretstores.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		target := &vaultSecretStore{
			client: nil,
			logger: nil,
		}

		if err := target.Init(t.Context(), m); err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, defaultVaultKVPrefix, target.vaultKVPrefix)
	})

	t.Run("if vaultKVUsePrefix is false ignore vaultKVPrefix", func(t *testing.T) {
		properties := map[string]string{
			"vaultKVPrefix":       "myCustomString",
			"vaultKVUsePrefix":    "false",
			"vaultTokenMountPath": expectedTokMountPath,
		}

		m := secretstores.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		target := &vaultSecretStore{
			client: nil,
			logger: nil,
		}

		if err := target.Init(t.Context(), m); err != nil {
			t.Fatal(err)
		}

		assert.Empty(t, target.vaultKVPrefix)
	})

	t.Run("if vaultKVUsePrefix is not castable to bool we treat it as False", func(t *testing.T) {
		properties := map[string]string{
			"vaultKVPrefix":       "myCustomString",
			"vaultKVUsePrefix":    "invalidSetting",
			"vaultTokenMountPath": expectedTokMountPath,
		}

		m := secretstores.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		meta := VaultMetadata{}
		kitmd.DecodeMetadata(m.Properties, &meta)

		assert.False(t, meta.VaultKVUsePrefix)
	})
}

func TestVaultTokenMountPathOrVaultTokenRequired(t *testing.T) {
	expectedTokMountPath, cleanUpFunc := createTokenMountPathFile(t)
	defer cleanUpFunc()

	t.Run("without vaultTokenMount or vaultToken", func(t *testing.T) {
		properties := map[string]string{}

		m := secretstores.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		target := &vaultSecretStore{
			client: nil,
			logger: nil,
		}

		err := target.Init(t.Context(), m)

		assert.Empty(t, target.vaultToken)
		assert.Empty(t, target.vaultTokenMountPath)
		require.Error(t, err)
		assert.Equal(t, "token mount path and token not set", err.Error())
	})

	t.Run("with vaultTokenMount", func(t *testing.T) {
		properties := map[string]string{
			"vaultTokenMountPath": expectedTokMountPath,
		}

		m := secretstores.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		target := &vaultSecretStore{
			client: nil,
			logger: nil,
		}

		if err := target.Init(t.Context(), m); err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, expectedTokenMountFileContents, target.vaultToken)
		assert.Equal(t, expectedTokMountPath, target.vaultTokenMountPath)
	})

	t.Run("with vaultToken", func(t *testing.T) {
		properties := map[string]string{
			"vaultToken": expectedTok,
		}

		m := secretstores.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		target := &vaultSecretStore{
			client: nil,
			logger: nil,
		}

		if err := target.Init(t.Context(), m); err != nil {
			t.Fatal(err)
		}

		assert.Empty(t, target.vaultTokenMountPath)
		assert.Equal(t, expectedTok, target.vaultToken)
	})

	t.Run("with vaultTokenMount and vaultToken", func(t *testing.T) {
		properties := map[string]string{
			"vaultToken":          expectedTok,
			"vaultTokenMountPath": expectedTokMountPath,
		}

		m := secretstores.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		target := &vaultSecretStore{
			client: nil,
			logger: nil,
		}

		err := target.Init(t.Context(), m)

		assert.Equal(t, expectedTok, target.vaultToken)
		assert.Equal(t, expectedTokMountPath, target.vaultTokenMountPath)
		require.Error(t, err)
		assert.Equal(t, "token mount path and token both set", err.Error())
	})
}

func TestDefaultVaultAddress(t *testing.T) {
	expectedTokMountPath, cleanUpFunc := createTokenMountPathFile(t)
	defer cleanUpFunc()

	t.Run("with blank vaultAddr", func(t *testing.T) {
		properties := map[string]string{
			"vaultTokenMountPath": expectedTokMountPath,
		}

		m := secretstores.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		target := &vaultSecretStore{
			client: nil,
			logger: nil,
		}

		if err := target.Init(t.Context(), m); err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, defaultVaultAddress, target.vaultAddress, "default was not set")
	})
}

func TestVaultAddressIgnoresAgentAddrEnvVar(t *testing.T) {
	t.Setenv("VAULT_AGENT_ADDR", "http://stale-agent-from-old-sidecar:8200")

	expectedTokMountPath, cleanUpFunc := createTokenMountPathFile(t)
	defer cleanUpFunc()

	properties := map[string]string{
		"vaultAddr":           "https://vault.example.com:8200",
		"vaultTokenMountPath": expectedTokMountPath,
	}

	m := secretstores.Metadata{
		Base: metadata.Base{Properties: properties},
	}

	target := &vaultSecretStore{
		client: nil,
		logger: nil,
	}

	require.NoError(t, target.Init(t.Context(), m))

	assert.Equal(t, "https://vault.example.com:8200", target.client.Address())
}

func TestVaultTLSIgnoresSkipVerifyEnvVar(t *testing.T) {
	t.Setenv("VAULT_SKIP_VERIFY", "true")

	expectedTokMountPath, cleanUpFunc := createTokenMountPathFile(t)
	defer cleanUpFunc()

	properties := map[string]string{
		"vaultTokenMountPath": expectedTokMountPath,
	}

	m := secretstores.Metadata{
		Base: metadata.Base{Properties: properties},
	}

	target := &vaultSecretStore{
		client: nil,
		logger: nil,
	}

	require.NoError(t, target.Init(t.Context(), m))

	transport, ok := target.client.CloneConfig().HttpClient.Transport.(*http.Transport)
	require.True(t, ok)
	assert.False(t, transport.TLSClientConfig.InsecureSkipVerify)
	assert.Contains(t, transport.TLSClientConfig.NextProtos, "h2")
}

func TestVaultSkipVerifyLogsWarning(t *testing.T) {
	expectedTokMountPath, cleanUpFunc := createTokenMountPathFile(t)
	defer cleanUpFunc()

	t.Run("skipVerify true logs a warning", func(t *testing.T) {
		var buf bytes.Buffer
		testLogger := logger.NewLogger("test")
		testLogger.SetOutput(&buf)

		target := newVaultSecretStore(testLogger)
		properties := map[string]string{
			"vaultTokenMountPath": expectedTokMountPath,
			"skipVerify":          "true",
		}
		require.NoError(t, target.Init(t.Context(), secretstores.Metadata{Base: metadata.Base{Properties: properties}}))

		assert.Contains(t, buf.String(), "skipVerify")
	})

	t.Run("skipVerify unset logs no warning", func(t *testing.T) {
		var buf bytes.Buffer
		testLogger := logger.NewLogger("test")
		testLogger.SetOutput(&buf)

		target := newVaultSecretStore(testLogger)
		properties := map[string]string{
			"vaultTokenMountPath": expectedTokMountPath,
		}
		require.NoError(t, target.Init(t.Context(), secretstores.Metadata{Base: metadata.Base{Properties: properties}}))

		assert.NotContains(t, buf.String(), "skipVerify")
	})
}

func TestVaultIgnoresNamespaceEnvVar(t *testing.T) {
	t.Setenv("VAULT_NAMESPACE", "some-other-namespace")

	expectedTokMountPath, cleanUpFunc := createTokenMountPathFile(t)
	defer cleanUpFunc()

	properties := map[string]string{
		"vaultTokenMountPath": expectedTokMountPath,
	}

	m := secretstores.Metadata{
		Base: metadata.Base{Properties: properties},
	}

	target := &vaultSecretStore{
		client: nil,
		logger: nil,
	}

	require.NoError(t, target.Init(t.Context(), m))

	assert.Empty(t, target.client.Headers().Get("X-Vault-Namespace"))
}

func TestVaultIgnoresProxyEnvVars(t *testing.T) {
	t.Setenv("VAULT_PROXY_ADDR", "http://127.0.0.1:1")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")

	expectedTokMountPath, cleanUpFunc := createTokenMountPathFile(t)
	defer cleanUpFunc()

	target := newVaultSecretStore(logger.NewLogger("test"))
	properties := map[string]string{
		"vaultTokenMountPath": expectedTokMountPath,
	}
	require.NoError(t, target.Init(t.Context(), secretstores.Metadata{Base: metadata.Base{Properties: properties}}))

	transport, ok := target.client.CloneConfig().HttpClient.Transport.(*http.Transport)
	require.True(t, ok)
	assert.Nil(t, transport.Proxy)
}

func TestVaultIgnoresClientEnvVars(t *testing.T) {
	t.Setenv("VAULT_MAX_RETRIES", "7")
	t.Setenv("VAULT_CLIENT_TIMEOUT", "1")
	t.Setenv("VAULT_RATE_LIMIT", "1")
	t.Setenv("VAULT_HEADERS", `{"X-Stray-Header":"from-env"}`)

	expectedTokMountPath, cleanUpFunc := createTokenMountPathFile(t)
	defer cleanUpFunc()

	target := newVaultSecretStore(logger.NewLogger("test"))
	properties := map[string]string{
		"vaultTokenMountPath": expectedTokMountPath,
	}
	require.NoError(t, target.Init(t.Context(), secretstores.Metadata{Base: metadata.Base{Properties: properties}}))

	config := target.client.CloneConfig()
	assert.Equal(t, vaultClientMaxRetries, config.MaxRetries)
	assert.Equal(t, vaultClientTimeout, config.Timeout)
	assert.Nil(t, config.Limiter)
	assert.Empty(t, target.client.Headers().Get("X-Stray-Header"))
	assert.Equal(t, "true", target.client.Headers().Get("X-Vault-Request"))
}

func TestVaultValueType(t *testing.T) {
	t.Run("valid vault value type map", func(t *testing.T) {
		properties := map[string]string{
			componentVaultToken: expectedTok,
			componentSkipVerify: "true",
			vaultValueType:      "map",
		}

		m := secretstores.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		target := &vaultSecretStore{
			client: nil,
			logger: logger.NewLogger("test"),
		}

		err := target.Init(t.Context(), m)
		require.NoError(t, err)
		assert.True(t, target.vaultValueType.isMapType())
	})

	t.Run("valid vault value type text", func(t *testing.T) {
		properties := map[string]string{
			componentVaultToken: expectedTok,
			componentSkipVerify: "true",
			vaultValueType:      "text",
		}

		m := secretstores.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		target := &vaultSecretStore{
			client: nil,
			logger: logger.NewLogger("test"),
		}

		err := target.Init(t.Context(), m)
		require.NoError(t, err)
		assert.False(t, target.vaultValueType.isMapType())
	})

	t.Run("empty vault value type", func(t *testing.T) {
		properties := map[string]string{
			componentVaultToken: expectedTok,
			componentSkipVerify: "true",
		}

		m := secretstores.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		target := &vaultSecretStore{
			client: nil,
			logger: logger.NewLogger("test"),
		}

		err := target.Init(t.Context(), m)
		require.NoError(t, err)
		assert.True(t, target.vaultValueType.isMapType())
	})

	t.Run("invalid vault value type", func(t *testing.T) {
		properties := map[string]string{
			componentVaultToken: expectedTok,
			componentSkipVerify: "true",
			vaultValueType:      "incorrect",
		}

		m := secretstores.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		target := &vaultSecretStore{
			client: nil,
			logger: nil,
		}

		err := target.Init(t.Context(), m)
		require.Error(t, err, "vault init error, invalid value type incorrect, accepted values are map or text")
	})
}

func getCertificate() []byte {
	certificateBytes, _ := base64.StdEncoding.DecodeString(certificate)

	return certificateBytes
}

func TestGetFeatures(t *testing.T) {
	initVaultWithVaultValueType := func(vaultValueType string) secretstores.SecretStore {
		properties := map[string]string{
			"vaultToken":     expectedTok,
			"skipVerify":     "true",
			"vaultValueType": vaultValueType,
		}

		m := secretstores.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		target := &vaultSecretStore{
			client: nil,
			logger: logger.NewLogger("test"),
		}

		_ = target.Init(t.Context(), m)

		return target
	}

	t.Run("Vault supports MULTIPLE_KEY_VALUES_PER_SECRET by default", func(t *testing.T) {
		// Yes, we are skipping initialization as feature retrieval doesn't depend on it for the default value
		s := NewHashiCorpVaultSecretStore(logger.NewLogger("test"))
		f := s.Features()
		assert.True(t, secretstores.FeatureMultipleKeyValuesPerSecret.IsPresent(f))
	})

	t.Run("Vault supports MULTIPLE_KEY_VALUES_PER_SECRET if configured with vaultValueType=map", func(t *testing.T) {
		// Yes, we are skipping initialization as feature retrieval doesn't depend on it for the default value
		s := initVaultWithVaultValueType("text")
		f := s.Features()
		assert.False(t, secretstores.FeatureMultipleKeyValuesPerSecret.IsPresent(f))
	})

	t.Run("Vault does not support MULTIPLE_KEY_VALUES_PER_SECRET if configured with vaultValueType=text", func(t *testing.T) {
		// Yes, we are skipping initialization as feature retrieval doesn't depend on it for the default value
		s := initVaultWithVaultValueType("text")
		f := s.Features()
		assert.False(t, secretstores.FeatureMultipleKeyValuesPerSecret.IsPresent(f))
	})
}

// waitForLifetimeWatchers must run before the test server closes: Close
// doesn't wait for watchers, and a request failing on a closed server is
// retried with backoff long enough for goleak in a later test to see it.
func waitForLifetimeWatchers(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool {
		buf := make([]byte, 1<<20)
		return !strings.Contains(string(buf[:runtime.Stack(buf, true)]), "(*LifetimeWatcher).Start")
	}, 10*time.Second, 20*time.Millisecond, "lifetime watcher goroutine did not exit")
}

func closeAndWaitForWatchers(t *testing.T, target *vaultSecretStore) {
	t.Helper()
	require.NoError(t, target.Close())
	waitForLifetimeWatchers(t)
}

func writeJSON(t *testing.T, w http.ResponseWriter, v interface{}) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	require.NoError(t, json.NewEncoder(w).Encode(v))
}

func TestKubernetesAuthMissingRole(t *testing.T) {
	target := newVaultSecretStore(logger.NewLogger("test"))
	properties := map[string]string{
		"vaultAuthMethod": "kubernetes",
	}

	err := target.Init(t.Context(), secretstores.Metadata{Base: metadata.Base{Properties: properties}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "vaultKubernetesRole")
}

func TestVaultInvalidAuthMethod(t *testing.T) {
	target := newVaultSecretStore(logger.NewLogger("test"))
	properties := map[string]string{
		"vaultAuthMethod": "bogus",
	}

	err := target.Init(t.Context(), secretstores.Metadata{Base: metadata.Base{Properties: properties}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "accepted values are token or kubernetes")
}

func TestKubernetesAuthConflictsWithToken(t *testing.T) {
	target := newVaultSecretStore(logger.NewLogger("test"))
	properties := map[string]string{
		"vaultAuthMethod":     "kubernetes",
		"vaultKubernetesRole": "my-role",
		"vaultToken":          "sometoken",
	}

	err := target.Init(t.Context(), secretstores.Metadata{Base: metadata.Base{Properties: properties}})
	require.Error(t, err)
}

func TestKubernetesAuthLoginFailure(t *testing.T) {
	tokenFile, cleanup := createTempFileWithContent(t, "test-jwt")
	defer cleanup()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		writeJSON(t, w, map[string]interface{}{"errors": []string{"permission denied"}})
	}))
	defer srv.Close()

	target := newVaultSecretStore(logger.NewLogger("test"))
	properties := map[string]string{
		"vaultAddr":                    srv.URL,
		"vaultAuthMethod":              "kubernetes",
		"vaultKubernetesRole":          "my-role",
		"vaultServiceAccountTokenPath": tokenFile,
	}

	err := target.Init(t.Context(), secretstores.Metadata{Base: metadata.Base{Properties: properties}})
	require.Error(t, err)
}

func TestKubernetesAuthSuccessAndGetSecret(t *testing.T) {
	tokenFile, cleanup := createTempFileWithContent(t, "test-jwt")
	defer cleanup()

	var loginRequests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/v1/auth/kubernetes/login":
			atomic.AddInt32(&loginRequests, 1)

			var body map[string]string
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			assert.Equal(t, "test-jwt", body["jwt"])
			assert.Equal(t, "my-role", body["role"])

			writeJSON(t, w, map[string]interface{}{
				"auth": map[string]interface{}{
					"client_token":   "test-vault-token",
					"lease_duration": 3600,
					"renewable":      true,
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/secret/data/dapr/mysecret":
			assert.Equal(t, "test-vault-token", r.Header.Get("X-Vault-Token"))
			writeJSON(t, w, map[string]interface{}{
				"data": map[string]interface{}{
					"data": map[string]interface{}{"key1": "value1"},
				},
			})
		case r.Method == http.MethodPut && r.URL.Path == "/v1/auth/token/renew-self":
			writeJSON(t, w, map[string]interface{}{
				"auth": map[string]interface{}{
					"client_token":   "test-vault-token",
					"lease_duration": 3600,
					"renewable":      true,
				},
			})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	target := newVaultSecretStore(logger.NewLogger("test"))
	properties := map[string]string{
		"vaultAddr":                    srv.URL,
		"vaultAuthMethod":              "kubernetes",
		"vaultKubernetesRole":          "my-role",
		"vaultServiceAccountTokenPath": tokenFile,
	}

	err := target.Init(t.Context(), secretstores.Metadata{Base: metadata.Base{Properties: properties}})
	require.NoError(t, err)
	defer closeAndWaitForWatchers(t, target)

	resp, err := target.GetSecret(t.Context(), secretstores.GetSecretRequest{Name: "mysecret"})
	require.NoError(t, err)
	assert.Equal(t, "value1", resp.Data["key1"])
	assert.EqualValues(t, 1, atomic.LoadInt32(&loginRequests))
}

func TestKubernetesAuthReauthenticatesWithFreshToken(t *testing.T) {
	tokenFile, cleanup := createTempFileWithContent(t, "token-v1")
	defer cleanup()

	var mu sync.Mutex
	var receivedJWTs []string
	secondLoginDone := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/v1/auth/kubernetes/login" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		var body map[string]string
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))

		mu.Lock()
		receivedJWTs = append(receivedJWTs, body["jwt"])
		n := len(receivedJWTs)
		mu.Unlock()

		writeJSON(t, w, map[string]interface{}{
			"auth": map[string]interface{}{
				"client_token":   fmt.Sprintf("token-%d", n),
				"lease_duration": 1,
				"renewable":      false,
			},
		})

		if n == 2 {
			close(secondLoginDone)
		}
	}))
	defer srv.Close()

	target := newVaultSecretStore(logger.NewLogger("test"))
	properties := map[string]string{
		"vaultAddr":                    srv.URL,
		"vaultAuthMethod":              "kubernetes",
		"vaultKubernetesRole":          "my-role",
		"vaultServiceAccountTokenPath": tokenFile,
	}

	err := target.Init(t.Context(), secretstores.Metadata{Base: metadata.Base{Properties: properties}})
	require.NoError(t, err)
	defer closeAndWaitForWatchers(t, target)

	require.NoError(t, os.WriteFile(tokenFile, []byte("token-v2"), 0o600))

	select {
	case <-secondLoginDone:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for second login")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, receivedJWTs, 2)
	assert.Equal(t, "token-v1", receivedJWTs[0])
	assert.Equal(t, "token-v2", receivedJWTs[1])
}

func TestKubernetesAuthCloseStopsRenewal(t *testing.T) {
	defer goleak.VerifyNone(t)

	tokenFile, cleanup := createTempFileWithContent(t, "test-jwt")
	defer cleanup()

	var loginCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/v1/auth/kubernetes/login" {
			atomic.AddInt32(&loginCount, 1)
			writeJSON(t, w, map[string]interface{}{
				"auth": map[string]interface{}{
					"client_token":   "test-token",
					"lease_duration": 3600,
					"renewable":      true,
				},
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	target := newVaultSecretStore(logger.NewLogger("test"))
	properties := map[string]string{
		"vaultAddr":                    srv.URL,
		"vaultAuthMethod":              "kubernetes",
		"vaultKubernetesRole":          "my-role",
		"vaultServiceAccountTokenPath": tokenFile,
	}

	err := target.Init(t.Context(), secretstores.Metadata{Base: metadata.Base{Properties: properties}})
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		_ = target.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close() did not return in time")
	}
	waitForLifetimeWatchers(t)

	countAfterClose := atomic.LoadInt32(&loginCount)
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, countAfterClose, atomic.LoadInt32(&loginCount), "no further requests should occur after Close()")
}

func TestGetSecretVersionQueryParam(t *testing.T) {
	tokenFile, cleanup := createTokenMountPathFile(t)
	defer cleanup()

	var gotVersion string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/secret/data/dapr/mysecret" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		gotVersion = r.URL.Query().Get("version")
		writeJSON(t, w, map[string]interface{}{
			"data": map[string]interface{}{
				"data": map[string]interface{}{"k": "v"},
			},
		})
	}))
	defer srv.Close()

	target := newVaultSecretStore(logger.NewLogger("test"))
	properties := map[string]string{
		"vaultAddr":           srv.URL,
		"vaultTokenMountPath": tokenFile,
	}
	require.NoError(t, target.Init(t.Context(), secretstores.Metadata{Base: metadata.Base{Properties: properties}}))

	_, err := target.GetSecret(t.Context(), secretstores.GetSecretRequest{Name: "mysecret"})
	require.NoError(t, err)
	assert.Equal(t, "0", gotVersion)

	_, err = target.GetSecret(t.Context(), secretstores.GetSecretRequest{
		Name:     "mysecret",
		Metadata: map[string]string{"version_id": "3"},
	})
	require.NoError(t, err)
	assert.Equal(t, "3", gotVersion)
}

func TestGetSecretMapTypeNullValueBecomesEmptyString(t *testing.T) {
	tokenFile, cleanup := createTokenMountPathFile(t)
	defer cleanup()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]interface{}{
			"data": map[string]interface{}{
				"data": map[string]interface{}{"a": "x", "b": nil},
			},
		})
	}))
	defer srv.Close()

	target := newVaultSecretStore(logger.NewLogger("test"))
	properties := map[string]string{
		"vaultAddr":           srv.URL,
		"vaultTokenMountPath": tokenFile,
	}
	require.NoError(t, target.Init(t.Context(), secretstores.Metadata{Base: metadata.Base{Properties: properties}}))

	resp, err := target.GetSecret(t.Context(), secretstores.GetSecretRequest{Name: "mysecret"})
	require.NoError(t, err)
	assert.Equal(t, "x", resp.Data["a"])
	assert.Empty(t, resp.Data["b"])
}

func TestGetSecretDeletedVersionIsNotFound(t *testing.T) {
	tokenFile, cleanup := createTokenMountPathFile(t)
	defer cleanup()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/secret/metadata/dapr" && r.URL.Query().Get("list") == "true":
			writeJSON(t, w, map[string]interface{}{
				"data": map[string]interface{}{
					"keys": []string{"alive", "deleted"},
				},
			})
		case r.URL.Path == "/v1/secret/data/dapr/alive":
			writeJSON(t, w, map[string]interface{}{
				"data": map[string]interface{}{
					"data": map[string]interface{}{"k": "v"},
				},
			})
		case r.URL.Path == "/v1/secret/data/dapr/deleted":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"data":{"data":null,"metadata":{"created_time":"2026-01-01T00:00:00Z","deletion_time":"2026-01-02T00:00:00Z","destroyed":false,"version":1}}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	target := newVaultSecretStore(logger.NewLogger("test"))
	properties := map[string]string{
		"vaultAddr":           srv.URL,
		"vaultTokenMountPath": tokenFile,
	}
	require.NoError(t, target.Init(t.Context(), secretstores.Metadata{Base: metadata.Base{Properties: properties}}))

	_, err := target.GetSecret(t.Context(), secretstores.GetSecretRequest{Name: "deleted"})
	require.ErrorIs(t, err, ErrNotFound)

	resp, err := target.BulkGetSecret(t.Context(), secretstores.BulkGetSecretRequest{})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"k": "v"}, resp.Data["alive"])
	assert.NotContains(t, resp.Data, "deleted")
}

func TestBulkGetSecretEmptyStoreErrors(t *testing.T) {
	tokenFile, cleanup := createTokenMountPathFile(t)
	defer cleanup()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	target := newVaultSecretStore(logger.NewLogger("test"))
	properties := map[string]string{
		"vaultAddr":           srv.URL,
		"vaultTokenMountPath": tokenFile,
	}
	require.NoError(t, target.Init(t.Context(), secretstores.Metadata{Base: metadata.Base{Properties: properties}}))

	_, err := target.BulkGetSecret(t.Context(), secretstores.BulkGetSecretRequest{})
	require.Error(t, err)
}

func TestBulkGetSecretKeysFieldEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		data      map[string]interface{}
		expectErr bool
	}{
		{name: "keys field absent", data: map[string]interface{}{}, expectErr: false},
		{name: "keys field null", data: map[string]interface{}{"keys": nil}, expectErr: false},
		{name: "keys field wrong type", data: map[string]interface{}{"keys": "not-an-array"}, expectErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokenFile, cleanup := createTokenMountPathFile(t)
			defer cleanup()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/secret/metadata/dapr" && r.URL.Query().Get("list") == "true" {
					writeJSON(t, w, map[string]interface{}{"data": tt.data})
					return
				}
				w.WriteHeader(http.StatusNotFound)
			}))
			defer srv.Close()

			target := newVaultSecretStore(logger.NewLogger("test"))
			properties := map[string]string{
				"vaultAddr":           srv.URL,
				"vaultTokenMountPath": tokenFile,
			}
			require.NoError(t, target.Init(t.Context(), secretstores.Metadata{Base: metadata.Base{Properties: properties}}))

			resp, err := target.BulkGetSecret(t.Context(), secretstores.BulkGetSecretRequest{})
			if tt.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Empty(t, resp.Data)
		})
	}
}

// The SDK returns a 404 with warnings as a non-nil secret without data.
func TestBulkGetSecretNotFoundWithWarningsErrors(t *testing.T) {
	tokenFile, cleanup := createTokenMountPathFile(t)
	defer cleanup()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]interface{}{
			"warnings": []string{"some warning"},
		}))
	}))
	defer srv.Close()

	target := newVaultSecretStore(logger.NewLogger("test"))
	properties := map[string]string{
		"vaultAddr":           srv.URL,
		"vaultTokenMountPath": tokenFile,
	}
	require.NoError(t, target.Init(t.Context(), secretstores.Metadata{Base: metadata.Base{Properties: properties}}))

	_, err := target.BulkGetSecret(t.Context(), secretstores.BulkGetSecretRequest{})
	require.Error(t, err)
}

func TestGetSecretBeforeInitReturnsError(t *testing.T) {
	target := newVaultSecretStore(logger.NewLogger("test"))

	_, err := target.GetSecret(t.Context(), secretstores.GetSecretRequest{Name: "mysecret"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not initialized")

	_, err = target.BulkGetSecret(t.Context(), secretstores.BulkGetSecretRequest{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not initialized")
}

// A zero-TTL non-renewable token makes the watcher return immediately.
func TestKubernetesAuthPacesRelogins(t *testing.T) {
	tokenFile, cleanup := createTempFileWithContent(t, "test-jwt")
	defer cleanup()

	var mu sync.Mutex
	var loginTimes []time.Time

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/v1/auth/kubernetes/login" {
			mu.Lock()
			loginTimes = append(loginTimes, time.Now())
			n := len(loginTimes)
			mu.Unlock()

			writeJSON(t, w, map[string]interface{}{
				"auth": map[string]interface{}{
					"client_token":   fmt.Sprintf("token-%d", n),
					"lease_duration": 0,
					"renewable":      false,
				},
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	target := newVaultSecretStore(logger.NewLogger("test"))
	properties := map[string]string{
		"vaultAddr":                    srv.URL,
		"vaultAuthMethod":              "kubernetes",
		"vaultKubernetesRole":          "my-role",
		"vaultServiceAccountTokenPath": tokenFile,
	}
	require.NoError(t, target.Init(t.Context(), secretstores.Metadata{Base: metadata.Base{Properties: properties}}))
	defer closeAndWaitForWatchers(t, target)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(loginTimes) >= 2
	}, reauthInitialInterval+5*time.Second, 50*time.Millisecond, "expected a second login")

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(loginTimes), 2)
	elapsed := loginTimes[1].Sub(loginTimes[0])
	assert.GreaterOrEqual(t, elapsed, reauthInitialInterval, "second login happened too soon after the first; renewal loop is not paced")
}

func TestKubernetesAuthCloseDuringBlockingInitDoesNotLeakGoroutine(t *testing.T) {
	defer goleak.VerifyNone(t)

	tokenFile, cleanup := createTempFileWithContent(t, "test-jwt")
	defer cleanup()

	loginReceived := make(chan struct{})
	releaseLogin := make(chan struct{})
	var loginCount int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/v1/auth/kubernetes/login" {
			atomic.AddInt32(&loginCount, 1)
			close(loginReceived)
			<-releaseLogin
			writeJSON(t, w, map[string]interface{}{
				"auth": map[string]interface{}{
					"client_token":   "test-token",
					"lease_duration": 3600,
					"renewable":      true,
				},
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	target := newVaultSecretStore(logger.NewLogger("test"))
	properties := map[string]string{
		"vaultAddr":                    srv.URL,
		"vaultAuthMethod":              "kubernetes",
		"vaultKubernetesRole":          "my-role",
		"vaultServiceAccountTokenPath": tokenFile,
	}

	initErrCh := make(chan error, 1)
	go func() {
		initErrCh <- target.Init(t.Context(), secretstores.Metadata{Base: metadata.Base{Properties: properties}})
	}()

	select {
	case <-loginReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the blocking login request")
	}

	closeDone := make(chan struct{})
	go func() {
		_ = target.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Close() did not return while Init() was still blocked on the login call")
	}

	close(releaseLogin)

	select {
	case err := <-initErrCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Init() did not return after the login unblocked")
	}

	time.Sleep(200 * time.Millisecond)
	assert.EqualValues(t, 1, atomic.LoadInt32(&loginCount), "no renewal goroutine should start once Close() already ran")
}

// daprd calls Close while holding the secret processor lock.
func TestKubernetesAuthCloseDoesNotBlockOnInFlightRenewal(t *testing.T) {
	defer goleak.VerifyNone(t)

	tokenFile, cleanup := createTempFileWithContent(t, "test-jwt")
	defer cleanup()

	renewReceived := make(chan struct{})
	releaseRenew := make(chan struct{})
	var renewReceivedOnce sync.Once

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/v1/auth/kubernetes/login":
			writeJSON(t, w, map[string]interface{}{
				"auth": map[string]interface{}{
					"client_token":   "test-token",
					"lease_duration": 3600,
					"renewable":      true,
				},
			})
		case r.Method == http.MethodPut && r.URL.Path == "/v1/auth/token/renew-self":
			renewReceivedOnce.Do(func() { close(renewReceived) })
			<-releaseRenew
			writeJSON(t, w, map[string]interface{}{
				"auth": map[string]interface{}{
					"client_token":   "test-token",
					"lease_duration": 3600,
					"renewable":      true,
				},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseRenew) }) }
	defer release() // before srv.Close, which waits for handlers

	target := newVaultSecretStore(logger.NewLogger("test"))
	properties := map[string]string{
		"vaultAddr":                    srv.URL,
		"vaultAuthMethod":              "kubernetes",
		"vaultKubernetesRole":          "my-role",
		"vaultServiceAccountTokenPath": tokenFile,
	}
	require.NoError(t, target.Init(t.Context(), secretstores.Metadata{Base: metadata.Base{Properties: properties}}))

	select {
	case <-renewReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the renewal loop's first renew-self call")
	}

	closeDone := make(chan struct{})
	go func() {
		_ = target.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close() blocked on the in-flight renewal")
	}

	release()
	waitForLifetimeWatchers(t)
}

func TestKubernetesAuthReloginsBeforeShortLeaseExpires(t *testing.T) {
	tokenFile, cleanup := createTempFileWithContent(t, "test-jwt")
	defer cleanup()

	var mu sync.Mutex
	var loginTimes []time.Time

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/v1/auth/kubernetes/login" {
			mu.Lock()
			loginTimes = append(loginTimes, time.Now())
			mu.Unlock()

			writeJSON(t, w, map[string]interface{}{
				"auth": map[string]interface{}{
					"client_token":   "test-token",
					"lease_duration": 4,
					"renewable":      false,
				},
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	target := newVaultSecretStore(logger.NewLogger("test"))
	properties := map[string]string{
		"vaultAddr":                    srv.URL,
		"vaultAuthMethod":              "kubernetes",
		"vaultKubernetesRole":          "my-role",
		"vaultServiceAccountTokenPath": tokenFile,
	}
	require.NoError(t, target.Init(t.Context(), secretstores.Metadata{Base: metadata.Base{Properties: properties}}))
	defer closeAndWaitForWatchers(t, target)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(loginTimes) >= 2
	}, 10*time.Second, 50*time.Millisecond, "expected a second login")

	mu.Lock()
	defer mu.Unlock()
	assert.Less(t, loginTimes[1].Sub(loginTimes[0]), 4*time.Second, "second login happened after the first token expired")
}

func TestKubernetesAuthTrimsServiceAccountToken(t *testing.T) {
	tokenFile, cleanup := createTempFileWithContent(t, "test-jwt\n")
	defer cleanup()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/v1/auth/kubernetes/login" {
			var body map[string]string
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			if body["jwt"] != "test-jwt" {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			writeJSON(t, w, map[string]interface{}{
				"auth": map[string]interface{}{
					"client_token":   "test-token",
					"lease_duration": 3600,
					"renewable":      false,
				},
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	target := newVaultSecretStore(logger.NewLogger("test"))
	properties := map[string]string{
		"vaultAddr":                    srv.URL,
		"vaultAuthMethod":              "kubernetes",
		"vaultKubernetesRole":          "my-role",
		"vaultServiceAccountTokenPath": tokenFile,
	}
	require.NoError(t, target.Init(t.Context(), secretstores.Metadata{Base: metadata.Base{Properties: properties}}))
	closeAndWaitForWatchers(t, target)
}
