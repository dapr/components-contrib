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

package azure

import (
	"encoding/base64"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	fakeTenantID = "14bec2db-7f9a-4f3d-97ca-2d384ac83389"
	fakeClientID = "04bec2db-7f9a-4f3d-97ca-3d384ac83389"

	// Base64 encoded test pfx cert - Expire date: 09/19/2119.
	testCert = "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"
)

func TestGetClientCert(t *testing.T) {
	settings, err := NewEnvironmentSettings(
		"keyvault",
		map[string]string{
			"azureCertificateFile":     "testfile",
			"azureCertificate":         "testcert",
			"azureCertificatePassword": "1234",
			"azureClientId":            fakeClientID,
			"azureTenantId":            fakeTenantID,
			"vaultName":                "vaultName",
		},
	)
	assert.NoError(t, err)

	testCertConfig, _ := settings.GetClientCert()

	assert.NotNil(t, testCertConfig.ClientCertificateConfig)
	assert.Equal(t, "testfile", testCertConfig.ClientCertificateConfig.CertificatePath)
	assert.Equal(t, []byte("testcert"), testCertConfig.CertificateData)
	assert.Equal(t, "1234", testCertConfig.ClientCertificateConfig.CertificatePassword)
	assert.Equal(t, fakeClientID, testCertConfig.ClientCertificateConfig.ClientID)
	assert.Equal(t, fakeTenantID, testCertConfig.ClientCertificateConfig.TenantID)
	assert.Equal(t, "https://vault.azure.net", testCertConfig.ClientCertificateConfig.Resource)
	assert.Equal(t, "https://login.microsoftonline.com/", testCertConfig.ClientCertificateConfig.AADEndpoint)
}

//nolint:gosec
func TestAuthorizorWithCertFile(t *testing.T) {
	testCertFileName := "./.cert.pfx"
	certBytes := getTestCert()
	err := ioutil.WriteFile(testCertFileName, certBytes, 0o644)
	assert.NoError(t, err)

	settings, err := NewEnvironmentSettings(
		"keyvault",
		map[string]string{
			"azureCertificateFile":     testCertFileName,
			"azureCertificatePassword": "",
			"azureClientId":            fakeClientID,
			"azureTenantId":            fakeTenantID,
			"vaultName":                "vaultName",
		},
	)
	assert.NoError(t, err)

	testCertConfig, _ := settings.GetClientCert()
	assert.NotNil(t, testCertConfig)
	assert.NotNil(t, testCertConfig.ClientCertificateConfig)

	authorizer, err := testCertConfig.Authorizer()
	assert.NoError(t, err)
	assert.NotNil(t, authorizer)

	err = os.Remove(testCertFileName)
	assert.NoError(t, err)
}

func TestAuthorizorWithCertBytes(t *testing.T) {
	t.Run("Certificate is valid", func(t *testing.T) {
		certBytes := getTestCert()

		settings, err := NewEnvironmentSettings(
			"keyvault",
			map[string]string{
				"azureCertificate":         string(certBytes),
				"azureCertificatePassword": "",
				"azureClientId":            fakeClientID,
				"azureTenantId":            fakeTenantID,
				"vaultName":                "vaultName",
			},
		)
		assert.NoError(t, err)

		testCertConfig, _ := settings.GetClientCert()
		assert.NotNil(t, testCertConfig)
		assert.NotNil(t, testCertConfig.ClientCertificateConfig)

		spt, err := testCertConfig.ServicePrincipalToken()

		assert.NoError(t, err)
		assert.NotNil(t, spt)
	})

	t.Run("Certificate is invalid", func(t *testing.T) {
		certBytes := getTestCert()

		settings, err := NewEnvironmentSettings(
			"keyvault",
			map[string]string{
				"azureCertificate":         string(certBytes[0:20]),
				"azureCertificatePassword": "",
				"azureClientId":            fakeClientID,
				"azureTenantId":            fakeTenantID,
				"vaultName":                "vaultName",
			},
		)
		assert.NoError(t, err)

		testCertConfig, _ := settings.GetClientCert()
		assert.NotNil(t, testCertConfig)
		assert.NotNil(t, testCertConfig.ClientCertificateConfig)

		_, err = testCertConfig.ServicePrincipalToken()
		assert.Error(t, err)
	})
}

func TestGetMSI(t *testing.T) {
	settings, err := NewEnvironmentSettings(
		"keyvault",
		map[string]string{
			"azureClientId": fakeClientID,
			"vaultName":     "vaultName",
		},
	)
	assert.NoError(t, err)

	testCertConfig := settings.GetMSI()

	assert.Equal(t, fakeClientID, testCertConfig.ClientID)
	assert.Equal(t, "https://vault.azure.net", testCertConfig.Resource)
}

func TestFallbackToMSI(t *testing.T) {
	os.Setenv("MSI_ENDPOINT", "test")
	defer os.Unsetenv("MSI_ENDPOINT")
	settings, err := NewEnvironmentSettings(
		"keyvault",
		map[string]string{
			"azureClientId": fakeClientID,
			"vaultName":     "vaultName",
		},
	)
	assert.NoError(t, err)

	spt, err := settings.GetServicePrincipalToken()

	assert.NotNil(t, spt)
	assert.NoError(t, err)
}

func TestAuthorizorWithMSI(t *testing.T) {
	os.Setenv("MSI_ENDPOINT", "test")
	defer os.Unsetenv("MSI_ENDPOINT")
	settings, err := NewEnvironmentSettings(
		"keyvault",
		map[string]string{
			"azureClientId": fakeClientID,
			"vaultName":     "vaultName",
		},
	)
	assert.NoError(t, err)

	testCertConfig := settings.GetMSI()
	assert.NotNil(t, testCertConfig)

	spt, err := testCertConfig.ServicePrincipalToken()
	assert.NoError(t, err)
	assert.NotNil(t, spt)
}

func TestAuthorizorWithMSIAndUserAssignedID(t *testing.T) {
	os.Setenv("MSI_ENDPOINT", "test")
	defer os.Unsetenv("MSI_ENDPOINT")
	settings, err := NewEnvironmentSettings(
		"keyvault",
		map[string]string{
			"azureClientId": fakeClientID,
			"vaultName":     "vaultName",
		},
	)
	assert.NoError(t, err)

	testCertConfig := settings.GetMSI()
	assert.NotNil(t, testCertConfig)

	spt, err := testCertConfig.ServicePrincipalToken()
	assert.NoError(t, err)
	assert.NotNil(t, spt)
}

func getTestCert() []byte {
	certBytes, _ := base64.StdEncoding.DecodeString(testCert)

	return certBytes
}
