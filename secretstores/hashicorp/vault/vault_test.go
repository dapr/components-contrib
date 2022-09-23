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
	"encoding/base64"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

const (
	// base64 encoded certificate.
	certificate          = "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURVakNDQWpvQ0NRRFlZdzdMeXN4VXRUQU5CZ2txaGtpRzl3MEJBUXNGQURCck1Rc3dDUVlEVlFRR0V3SkQKUVRFWk1CY0dBMVVFQ0F3UVFuSnBkR2x6YUNCRGIyeDFiV0pwWVRFU01CQUdBMVVFQnd3SlZtRnVZMjkxZG1WeQpNUk13RVFZRFZRUUtEQXB0YVhOb2NtRmpiM0p3TVJnd0ZnWURWUVFEREE5MllYVnNkSEJ5YjJwbFkzUXVhVzh3CkhoY05NVGt4TVRBeE1UQTBPREV5V2hjTk1qQXhNRE14TVRBME9ERXlXakJyTVFzd0NRWURWUVFHRXdKRFFURVoKTUJjR0ExVUVDQXdRUW5KcGRHbHphQ0JEYjJ4MWJXSnBZVEVTTUJBR0ExVUVCd3dKVm1GdVkyOTFkbVZ5TVJNdwpFUVlEVlFRS0RBcHRhWE5vY21GamIzSndNUmd3RmdZRFZRUUREQTkyWVhWc2RIQnliMnBsWTNRdWFXOHdnZ0VpCk1BMEdDU3FHU0liM0RRRUJBUVVBQTRJQkR3QXdnZ0VLQW9JQkFRQ3JtaitTTmtGUHEvK2FXUFV1MlpFamtSK3AKTm1PeEVNSnZZcGhHNkJvRFAySE9ZbGRzdk9FWkRkbTBpWFlmeFIwZm5rUmtTMWEzSlZiYmhINWJnTElKb0dxcwo5aWpzN2hyQ0Rrdk9uRWxpUEZuc05pQ2NWNDNxNkZYaFMvNFpoNGpOMnlyUkU2UmZiS1BEeUw0a282NkFhSld1CnVkTldKVWpzSFZBSWowZHlnTXFKYm0rT29iSzk5ckUxcDg5Z3RNUStJdzFkWnUvUFF4SjlYOStMeXdxZUxPckQKOWhpNWkxajNFUUp2RXQxSVUzclEwc2E0NU5zZkt4YzEwZjdhTjJuSDQzSnhnMVRiZXNPOWYrcWlyeDBHYmVSYQpyVmNaazNVaFc2cHZmam9XbDBEc0NwNTJwZDBQN05rUmhmak44b2RMN0h3bFVIc1NqemlSYytsTG5YREJBZ01CCkFBRXdEUVlKS29aSWh2Y05BUUVMQlFBRGdnRUJBSVdKdmRPZ01PUnQxWk53SENkNTNieTlkMlBkcW5tWHFZZ20KNDZHK2Fvb1dSeTJKMEMwS3ZOVGZGbEJFOUlydzNXUTVNMnpqY25qSUp5bzNLRUM5TDdPMnQ1WC9LTGVDck5ZVgpIc1d4cU5BTVBGY2VBa09HT0I1TThGVllkdjJTaVV2UDJjMEZQSzc2WFVzcVNkdnRsWGFkTk5ENzE3T0NTNm0yCnBIVjh1NWJNd1VmR2NCVFpEV2o4bjIzRVdHaXdnYkJkdDc3Z3h3YWc5NTROZkM2Ny9nSUc5ZlRrTTQ4aVJCUzEKc0NGYVBjMkFIT3hiMSs0ajVCMVY2Z29iZDZYWkFvbHdNaTNHUUtkbEM1NXZNeTNwK09WbDNNbEc3RWNTVUpMdApwZ2ZKaWw3L3dTWWhpUnhJU3hrYkk5cWhvNEwzZm5PZVB3clFVd2FzU1ZiL1lxbHZ2WHM9Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K"
	expectedTok          = "myRootToken"
	expectedTokMountPath = "./vault.txt"
)

func TestReadVaultToken(t *testing.T) {
	t.Run("read correct token", func(t *testing.T) {
		dir := os.TempDir()
		f, err := os.CreateTemp(dir, "vault-token")
		assert.NoError(t, err)
		fileName := f.Name()
		defer os.Remove(fileName)

		tokenString := "thisisnottheroottoken"
		_, err = f.WriteString(tokenString)
		assert.NoError(t, err)

		v := vaultSecretStore{
			vaultTokenMountPath: f.Name(),
		}

		err = v.initVaultToken()
		assert.Nil(t, err)
		assert.Equal(t, tokenString, v.vaultToken)
	})

	t.Run("read incorrect token", func(t *testing.T) {
		dir := os.TempDir()
		f, err := os.CreateTemp(dir, "vault-token")
		assert.NoError(t, err)
		fileName := f.Name()
		defer os.Remove(fileName)

		tokenString := "thisisnottheroottoken"
		_, err = f.WriteString(tokenString)
		assert.NoError(t, err)

		v := vaultSecretStore{
			vaultTokenMountPath: f.Name(),
		}
		err = v.initVaultToken()
		assert.Nil(t, err)
		assert.NotEqual(t, "thisistheroottoken", v.vaultToken)
	})

	t.Run("read token from vaultToken", func(t *testing.T) {
		v := vaultSecretStore{
			vaultToken: expectedTok,
		}

		err := v.initVaultToken()

		assert.Nil(t, err)
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

		tlsConfig := metadataToTLSConfig(m.Properties)
		skipVerify, err := strconv.ParseBool(properties["skipVerify"])
		assert.Nil(t, err)
		assert.Equal(t, properties["caCert"], tlsConfig.vaultCACert)
		assert.Equal(t, skipVerify, tlsConfig.vaultSkipVerify)
		assert.Equal(t, properties["tlsServerName"], tlsConfig.vaultServerName)
	})
}

func TestVaultEnginePath(t *testing.T) {
	t.Run("without engine path config", func(t *testing.T) {
		v := vaultSecretStore{}

		err := v.Init(secretstores.Metadata{Base: metadata.Base{Properties: map[string]string{componentVaultToken: expectedTok, "skipVerify": "true"}}})
		assert.Nil(t, err)
		assert.Equal(t, v.vaultEnginePath, defaultVaultEnginePath)
	})

	t.Run("with engine path config", func(t *testing.T) {
		v := vaultSecretStore{}

		err := v.Init(secretstores.Metadata{Base: metadata.Base{Properties: map[string]string{componentVaultToken: expectedTok, "skipVerify": "true", vaultEnginePath: "kv"}}})
		assert.Nil(t, err)
		assert.Equal(t, v.vaultEnginePath, "kv")
	})
}

func TestVaultTokenPrefix(t *testing.T) {
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

		// This call will throw an error on Windows systems because of the of
		// the call x509.SystemCertPool() because system root pool is not
		// available on Windows so ignore the error for when the tests are run
		// on the Windows platform during CI
		_ = target.Init(m)

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

		// This call will throw an error on Windows systems because of the of
		// the call x509.SystemCertPool() because system root pool is not
		// available on Windows so ignore the error for when the tests are run
		// on the Windows platform during CI
		_ = target.Init(m)

		assert.Equal(t, "", target.vaultKVPrefix)
	})

	t.Run("if vaultKVUsePrefix is not castable to bool return error", func(t *testing.T) {
		properties := map[string]string{
			"vaultKVPrefix":       "myCustomString",
			"vaultKVUsePrefix":    "invalidSetting",
			"vaultTokenMountPath": expectedTokMountPath,
		}

		m := secretstores.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		target := &vaultSecretStore{
			client: nil,
			logger: nil,
		}

		// This call will throw an error on Windows systems because of the of
		// the call x509.SystemCertPool() because system root pool is not
		// available on Windows so ignore the error for when the tests are run
		// on the Windows platform during CI
		err := target.Init(m)

		assert.NotNil(t, err)
	})
}

func TestVaultTokenMountPathOrVaultTokenRequired(t *testing.T) {
	t.Run("without vaultTokenMount or vaultToken", func(t *testing.T) {
		properties := map[string]string{}

		m := secretstores.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		target := &vaultSecretStore{
			client: nil,
			logger: nil,
		}

		err := target.Init(m)

		assert.Equal(t, "", target.vaultToken)
		assert.Equal(t, "", target.vaultTokenMountPath)
		assert.NotNil(t, err)
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

		// This call will throw an error on Windows systems because of the of
		// the call x509.SystemCertPool() because system root pool is not
		// available on Windows so ignore the error for when the tests are run
		// on the Windows platform during CI
		_ = target.Init(m)

		assert.Equal(t, "", target.vaultToken)
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

		// This call will throw an error on Windows systems because of the of
		// the call x509.SystemCertPool() because system root pool is not
		// available on Windows so ignore the error for when the tests are run
		// on the Windows platform during CI
		_ = target.Init(m)

		assert.Equal(t, "", target.vaultTokenMountPath)
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

		err := target.Init(m)

		assert.Equal(t, expectedTok, target.vaultToken)
		assert.Equal(t, expectedTokMountPath, target.vaultTokenMountPath)
		assert.NotNil(t, err)
		assert.Equal(t, "token mount path and token both set", err.Error())
	})
}

func TestDefaultVaultAddress(t *testing.T) {
	t.Run("with blank vaultAddr", func(t *testing.T) {
		properties := map[string]string{
			"vaultTokenMountPath": "./vault.txt",
		}

		m := secretstores.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		target := &vaultSecretStore{
			client: nil,
			logger: nil,
		}

		// This call will throw an error on Windows systems because of the of
		// the call x509.SystemCertPool() because system root pool is not
		// available on Windows so ignore the error for when the tests are run
		// on the Windows platform during CI
		_ = target.Init(m)

		assert.Equal(t, defaultVaultAddress, target.vaultAddress, "default was not set")
	})
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
			logger: nil,
		}

		err := target.Init(m)
		assert.Nil(t, err)
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
			logger: nil,
		}

		err := target.Init(m)
		assert.Nil(t, err)
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
			logger: nil,
		}

		err := target.Init(m)
		assert.Nil(t, err)
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

		err := target.Init(m)
		assert.Error(t, err, "vault init error, invalid value type incorrect, accepted values are map or text")
	})
}

func getCertificate() []byte {
	certificateBytes, _ := base64.StdEncoding.DecodeString(certificate)

	return certificateBytes
}

func TestGetFeatures(t *testing.T) {
	s := NewHashiCorpVaultSecretStore(logger.NewLogger("test"))
	// Yes, we are skipping initialization as feature retrieval doesn't depend on it.
	t.Run("Vault supports MULTIPLE_KEY_VALUES_PER_SECRET", func(t *testing.T) {
		f := s.Features()
		assert.True(t, secretstores.FeatureMultipleKeyValuesPerSecret.IsPresent(f))
	})
}
