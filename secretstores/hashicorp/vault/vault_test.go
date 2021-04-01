// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package vault

import (
	"encoding/base64"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/stretchr/testify/assert"
)

const (
	// base64 encoded certificate
	certificate = "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURVakNDQWpvQ0NRRFlZdzdMeXN4VXRUQU5CZ2txaGtpRzl3MEJBUXNGQURCck1Rc3dDUVlEVlFRR0V3SkQKUVRFWk1CY0dBMVVFQ0F3UVFuSnBkR2x6YUNCRGIyeDFiV0pwWVRFU01CQUdBMVVFQnd3SlZtRnVZMjkxZG1WeQpNUk13RVFZRFZRUUtEQXB0YVhOb2NtRmpiM0p3TVJnd0ZnWURWUVFEREE5MllYVnNkSEJ5YjJwbFkzUXVhVzh3CkhoY05NVGt4TVRBeE1UQTBPREV5V2hjTk1qQXhNRE14TVRBME9ERXlXakJyTVFzd0NRWURWUVFHRXdKRFFURVoKTUJjR0ExVUVDQXdRUW5KcGRHbHphQ0JEYjJ4MWJXSnBZVEVTTUJBR0ExVUVCd3dKVm1GdVkyOTFkbVZ5TVJNdwpFUVlEVlFRS0RBcHRhWE5vY21GamIzSndNUmd3RmdZRFZRUUREQTkyWVhWc2RIQnliMnBsWTNRdWFXOHdnZ0VpCk1BMEdDU3FHU0liM0RRRUJBUVVBQTRJQkR3QXdnZ0VLQW9JQkFRQ3JtaitTTmtGUHEvK2FXUFV1MlpFamtSK3AKTm1PeEVNSnZZcGhHNkJvRFAySE9ZbGRzdk9FWkRkbTBpWFlmeFIwZm5rUmtTMWEzSlZiYmhINWJnTElKb0dxcwo5aWpzN2hyQ0Rrdk9uRWxpUEZuc05pQ2NWNDNxNkZYaFMvNFpoNGpOMnlyUkU2UmZiS1BEeUw0a282NkFhSld1CnVkTldKVWpzSFZBSWowZHlnTXFKYm0rT29iSzk5ckUxcDg5Z3RNUStJdzFkWnUvUFF4SjlYOStMeXdxZUxPckQKOWhpNWkxajNFUUp2RXQxSVUzclEwc2E0NU5zZkt4YzEwZjdhTjJuSDQzSnhnMVRiZXNPOWYrcWlyeDBHYmVSYQpyVmNaazNVaFc2cHZmam9XbDBEc0NwNTJwZDBQN05rUmhmak44b2RMN0h3bFVIc1NqemlSYytsTG5YREJBZ01CCkFBRXdEUVlKS29aSWh2Y05BUUVMQlFBRGdnRUJBSVdKdmRPZ01PUnQxWk53SENkNTNieTlkMlBkcW5tWHFZZ20KNDZHK2Fvb1dSeTJKMEMwS3ZOVGZGbEJFOUlydzNXUTVNMnpqY25qSUp5bzNLRUM5TDdPMnQ1WC9LTGVDck5ZVgpIc1d4cU5BTVBGY2VBa09HT0I1TThGVllkdjJTaVV2UDJjMEZQSzc2WFVzcVNkdnRsWGFkTk5ENzE3T0NTNm0yCnBIVjh1NWJNd1VmR2NCVFpEV2o4bjIzRVdHaXdnYkJkdDc3Z3h3YWc5NTROZkM2Ny9nSUc5ZlRrTTQ4aVJCUzEKc0NGYVBjMkFIT3hiMSs0ajVCMVY2Z29iZDZYWkFvbHdNaTNHUUtkbEM1NXZNeTNwK09WbDNNbEc3RWNTVUpMdApwZ2ZKaWw3L3dTWWhpUnhJU3hrYkk5cWhvNEwzZm5PZVB3clFVd2FzU1ZiL1lxbHZ2WHM9Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K"
)

func TestReadVaultToken(t *testing.T) {
	t.Run("read correct token", func(t *testing.T) {
		dir := os.TempDir()
		f, err := ioutil.TempFile(dir, "vault-token")
		assert.NoError(t, err)
		fileName := f.Name()
		defer os.Remove(fileName)

		tokenString := "thisisnottheroottoken"
		_, err = f.WriteString(tokenString)
		assert.NoError(t, err)

		v := vaultSecretStore{
			vaultTokenMountPath: f.Name(),
		}

		token, err := v.readVaultToken()
		assert.Nil(t, err)
		assert.Equal(t, tokenString, token)
	})

	t.Run("read incorrect token", func(t *testing.T) {
		dir := os.TempDir()
		f, err := ioutil.TempFile(dir, "vault-token")
		assert.NoError(t, err)
		fileName := f.Name()
		defer os.Remove(fileName)

		tokenString := "thisisnottheroottoken"
		_, err = f.WriteString(tokenString)
		assert.NoError(t, err)

		v := vaultSecretStore{
			vaultTokenMountPath: f.Name(),
		}
		token, err := v.readVaultToken()
		assert.Nil(t, err)
		assert.NotEqual(t, "thisistheroottoken", token)
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
			Properties: properties,
		}

		tlsConfig := metadataToTLSConfig(m.Properties)
		skipVerify, err := strconv.ParseBool(properties["skipVerify"])
		assert.Nil(t, err)
		assert.Equal(t, properties["caCert"], tlsConfig.vaultCACert)
		assert.Equal(t, skipVerify, tlsConfig.vaultSkipVerify)
		assert.Equal(t, properties["tlsServerName"], tlsConfig.vaultServerName)
	})
}

func TestDefaultVaultAddress(t *testing.T) {
	t.Run("with blank vaultAddr", func(t *testing.T) {
		properties := map[string]string{
			"vaultTokenMountPath": "./vault.txt",
		}

		m := secretstores.Metadata{
			Properties: properties,
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

func getCertificate() []byte {
	certificateBytes, _ := base64.StdEncoding.DecodeString(certificate)

	return certificateBytes
}
