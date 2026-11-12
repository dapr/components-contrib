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
	"context"
	"crypto"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/spiffe/go-spiffe/v2/svid/jwtsvid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	spiffecontext "github.com/dapr/kit/crypto/spiffe/context"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) Do(req *http.Request) (*http.Response, error) {
	return f(req)
}

type jwtSourceFunc func(context.Context, jwtsvid.Params) (*jwtsvid.SVID, error)

func (f jwtSourceFunc) FetchJWTSVID(ctx context.Context, params jwtsvid.Params) (*jwtsvid.SVID, error) {
	return f(ctx, params)
}

const (
	fakeTenantID = "14bec2db-7f9a-4f3d-97ca-2d384ac83389"
	fakeClientID = "04bec2db-7f9a-4f3d-97ca-3d384ac83389"

	// Base64 encoded test pfx cert - Expire date: 09/19/2119.
	testCert = "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"
)

func TestGetClientCert(t *testing.T) {
	settings, err := NewEnvironmentSettings(
		map[string]string{
			"azureCertificateFile":     "testfile",
			"azureCertificate":         "testcert",
			"azureCertificatePassword": "1234",
			"azureClientId":            fakeClientID,
			"azureTenantId":            fakeTenantID,
			"vaultName":                "vaultName",
		},
	)
	require.NoError(t, err)

	testCertConfig, _ := settings.GetClientCert()

	assert.Equal(t, "testfile", testCertConfig.CertificatePath)
	assert.Equal(t, []byte("testcert"), testCertConfig.CertificateData)
	assert.Equal(t, "1234", testCertConfig.CertificatePassword)
	assert.Equal(t, fakeClientID, testCertConfig.ClientID)
	assert.Equal(t, fakeTenantID, testCertConfig.TenantID)
	require.NotNil(t, testCertConfig.AzureCloud)
	assert.Equal(t, "https://login.microsoftonline.com/", testCertConfig.AzureCloud.ActiveDirectoryAuthorityHost)
	assert.Equal(t, "core.windows.net", settings.EndpointSuffix(ServiceAzureStorage))
}

func TestAzureCloud(t *testing.T) {
	settings, err := NewEnvironmentSettings(
		map[string]string{
			"azureCertificateFile":     "testfile",
			"azureCertificate":         "testcert",
			"azureCertificatePassword": "1234",
			"azureClientId":            fakeClientID,
			"azureTenantId":            fakeTenantID,
			"vaultName":                "vaultName",
			"azureEnvironment":         "AzureChina",
		},
	)
	require.NoError(t, err)

	testCertConfig, _ := settings.GetClientCert()

	assert.Equal(t, "testfile", testCertConfig.CertificatePath)
	assert.Equal(t, []byte("testcert"), testCertConfig.CertificateData)
	assert.Equal(t, "1234", testCertConfig.CertificatePassword)
	assert.Equal(t, fakeClientID, testCertConfig.ClientID)
	assert.Equal(t, fakeTenantID, testCertConfig.TenantID)
	require.NotNil(t, testCertConfig.AzureCloud)
	assert.Equal(t, "https://login.chinacloudapi.cn/", testCertConfig.AzureCloud.ActiveDirectoryAuthorityHost)
	assert.Equal(t, "core.chinacloudapi.cn", settings.EndpointSuffix(ServiceAzureStorage))
}

// TestDisableInstanceDiscovery catches fail-open authority validation and unwanted public
// discovery requests that token acquisition against a connected Azure cloud would not detect.
func TestDisableInstanceDiscovery(t *testing.T) {
	const authorityHost = "private.example"
	setWorkloadIdentityEnvironment(t, "https://"+authorityHost+"/")

	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256"}`))
	claims := base64.RawURLEncoding.EncodeToString(fmt.Appendf(nil,
		`{"sub":"spiffe://example.org/workload","aud":["%s"],"exp":%d}`,
		AzureADTokenExchangeAudience, time.Now().Add(time.Hour).Unix()))
	svid, err := jwtsvid.ParseInsecure(header+"."+claims+".c2lnbmF0dXJl", []string{AzureADTokenExchangeAudience})
	require.NoError(t, err)
	ctx := spiffecontext.WithJWT(t.Context(), jwtSourceFunc(func(context.Context, jwtsvid.Params) (*jwtsvid.SVID, error) {
		return svid, nil
	}))

	tests := []struct {
		name     string
		value    string
		setValue bool
		disabled bool
	}{
		{name: "missing"},
		{name: "empty", setValue: true},
		{name: "whitespace", value: " \t ", setValue: true},
		{name: "false", value: "false", setValue: true},
		{name: "mixed case false", value: "FaLsE", setValue: true},
		{name: "unrecognized", value: "enabled", setValue: true},
		{name: "zero", value: "0", setValue: true},
		{name: "true", value: "true", setValue: true, disabled: true},
		{name: "mixed case and whitespace", value: " TrUe ", setValue: true, disabled: true},
		{name: "y", value: "y", setValue: true, disabled: true},
		{name: "yes", value: "yes", setValue: true, disabled: true},
		{name: "t", value: "t", setValue: true, disabled: true},
		{name: "on", value: "on", setValue: true, disabled: true},
		{name: "one", value: "1", setValue: true, disabled: true},
	}

	for _, method := range []string{"ClientCredentials", "ClientCertificate", "WorkloadIdentity", "SPIFFEWorkloadIdentity"} {
		t.Run(method, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					var requestHosts []string
					setCredentialTransport(t, authorityHost, transportFunc(func(req *http.Request) (*http.Response, error) {
						requestHosts = append(requestHosts, req.URL.Host)
						status := http.StatusOK
						var body string
						switch {
						case req.URL.Hostname() != authorityHost:
							status = http.StatusBadRequest
							body = `{"error":"public_discovery_unavailable"}`
						case req.Method == http.MethodGet:
							body = fmt.Sprintf(
								`{"authorization_endpoint":"https://%[1]s/%[2]s/oauth2/v2.0/authorize","token_endpoint":"https://%[1]s/%[2]s/oauth2/v2.0/token","issuer":"https://%[1]s/%[2]s/v2.0"}`,
								authorityHost, fakeTenantID)
						case req.Method == http.MethodPost:
							body = `{"access_token":"test-token","expires_in":3600,"token_type":"Bearer"}`
						default:
							return nil, fmt.Errorf("unexpected request method %s", req.Method)
						}
						return &http.Response{
							StatusCode: status,
							Header:     http.Header{"Content-Type": []string{"application/json"}},
							Body:       io.NopCloser(strings.NewReader(body)),
							Request:    req,
						}, nil
					}))

					properties := map[string]string{
						"azureAuthMethods":  method,
						"azureTenantId":     fakeTenantID,
						"azureClientId":     fakeClientID,
						"azureClientSecret": "test-secret",
						"azureCertificate":  string(getTestCert()),
					}
					if test.setValue {
						properties["azureDisableInstanceDiscovery"] = test.value
					}
					settings, err := NewEnvironmentSettings(properties)
					require.NoError(t, err)
					credential, err := settings.GetTokenCredential()
					require.NoError(t, err)

					token, err := credential.GetToken(ctx, policy.TokenRequestOptions{
						Scopes: []string{"https://storage.azure.com/.default"},
					})
					if !test.disabled {
						require.Error(t, err)
						require.Contains(t, requestHosts, "login.microsoftonline.com")
						return
					}
					require.NoError(t, err)
					require.Equal(t, "test-token", token.Token)
					require.NotEmpty(t, requestHosts)
					for _, host := range requestHosts {
						require.Equal(t, authorityHost, host)
					}
				})
			}
		})
	}
}

func setCredentialTransport(t *testing.T, authorityHost string, transport policy.Transporter) {
	t.Helper()
	originalSecret := newClientSecretCredential
	originalCertificate := newClientCertificateCredential
	originalWorkload := newWorkloadIdentityCredential
	originalAssertion := newClientAssertionCredential
	t.Cleanup(func() {
		newClientSecretCredential = originalSecret
		newClientCertificateCredential = originalCertificate
		newWorkloadIdentityCredential = originalWorkload
		newClientAssertionCredential = originalAssertion
	})
	privateCloud := cloud.Configuration{ActiveDirectoryAuthorityHost: "https://" + authorityHost + "/"}
	newClientSecretCredential = func(tenantID, clientID, secret string, options *azidentity.ClientSecretCredentialOptions) (*azidentity.ClientSecretCredential, error) {
		options.Transport = transport
		options.Cloud = privateCloud
		return originalSecret(tenantID, clientID, secret, options)
	}
	newClientCertificateCredential = func(tenantID, clientID string, certs []*x509.Certificate, key crypto.PrivateKey, options *azidentity.ClientCertificateCredentialOptions) (*azidentity.ClientCertificateCredential, error) {
		options.Transport = transport
		options.Cloud = privateCloud
		return originalCertificate(tenantID, clientID, certs, key, options)
	}
	newWorkloadIdentityCredential = func(options *azidentity.WorkloadIdentityCredentialOptions) (*azidentity.WorkloadIdentityCredential, error) {
		options.Transport = transport
		return originalWorkload(options)
	}
	newClientAssertionCredential = func(tenantID, clientID string, assertion func(context.Context) (string, error), options *azidentity.ClientAssertionCredentialOptions) (*azidentity.ClientAssertionCredential, error) {
		options.Transport = transport
		options.Cloud = privateCloud
		return originalAssertion(tenantID, clientID, assertion, options)
	}
}

func TestEndpointSuffix(t *testing.T) {
	es := EnvironmentSettings{}

	es.Cloud = nil
	assert.Equal(t, "vault.azure.net", es.EndpointSuffix(ServiceAzureKeyVault))

	es.Cloud = &cloud.AzurePublic
	assert.Equal(t, "vault.azure.net", es.EndpointSuffix(ServiceAzureKeyVault))

	es.Cloud = &cloud.AzureGovernment
	assert.Equal(t, "vault.usgovcloudapi.net", es.EndpointSuffix(ServiceAzureKeyVault))
}

//nolint:gosec
func TestAuthorizorWithCertFile(t *testing.T) {
	testCertFileName := "./.cert.pfx"
	certBytes := getTestCert()
	err := os.WriteFile(testCertFileName, certBytes, 0o644)
	require.NoError(t, err)

	settings, err := NewEnvironmentSettings(
		map[string]string{
			"azureCertificateFile":     testCertFileName,
			"azureCertificatePassword": "",
			"azureClientId":            fakeClientID,
			"azureTenantId":            fakeTenantID,
			"vaultName":                "vaultName",
		},
	)
	require.NoError(t, err)

	testCertConfig, _ := settings.GetClientCert()

	spt, err := testCertConfig.GetTokenCredential()
	require.NoError(t, err)
	assert.NotNil(t, spt)

	err = os.Remove(testCertFileName)
	require.NoError(t, err)
}

func TestAuthorizorWithCertBytes(t *testing.T) {
	t.Run("Certificate is valid", func(t *testing.T) {
		certBytes := getTestCert()

		settings, err := NewEnvironmentSettings(
			map[string]string{
				"azureCertificate":         string(certBytes),
				"azureCertificatePassword": "",
				"azureClientId":            fakeClientID,
				"azureTenantId":            fakeTenantID,
				"vaultName":                "vaultName",
			},
		)
		require.NoError(t, err)

		testCertConfig, _ := settings.GetClientCert()
		assert.NotNil(t, testCertConfig)

		spt, err := testCertConfig.GetTokenCredential()
		require.NoError(t, err)
		assert.NotNil(t, spt)
	})

	t.Run("Certificate is invalid", func(t *testing.T) {
		certBytes := getTestCert()

		settings, err := NewEnvironmentSettings(
			map[string]string{
				"azureCertificate":         string(certBytes[0:20]),
				"azureCertificatePassword": "",
				"azureClientId":            fakeClientID,
				"azureTenantId":            fakeTenantID,
				"vaultName":                "vaultName",
			},
		)
		require.NoError(t, err)

		testCertConfig, _ := settings.GetClientCert()
		assert.NotNil(t, testCertConfig)

		_, err = testCertConfig.GetTokenCredential()
		require.Error(t, err)
	})
}

func TestGetMSI(t *testing.T) {
	settings, err := NewEnvironmentSettings(
		map[string]string{
			"azureClientId": fakeClientID,
			"vaultName":     "vaultName",
		},
	)
	require.NoError(t, err)

	testCertConfig := settings.GetMSI()

	assert.Equal(t, fakeClientID, testCertConfig.ClientID)
}

func TestFallbackToMSI(t *testing.T) {
	t.Setenv("MSI_ENDPOINT", "test")
	defer os.Unsetenv("MSI_ENDPOINT")
	settings, err := NewEnvironmentSettings(
		map[string]string{
			"azureClientId": fakeClientID,
			"vaultName":     "vaultName",
		},
	)
	require.NoError(t, err)

	spt, err := settings.GetTokenCredential()
	require.NoError(t, err)
	assert.NotNil(t, spt)
}

func TestAuthorizorWithMSI(t *testing.T) {
	t.Setenv("MSI_ENDPOINT", "test")
	defer os.Unsetenv("MSI_ENDPOINT")
	settings, err := NewEnvironmentSettings(
		map[string]string{
			"azureClientId": fakeClientID,
			"vaultName":     "vaultName",
		},
	)
	require.NoError(t, err)

	testCertConfig := settings.GetMSI()
	assert.NotNil(t, testCertConfig)

	spt, err := settings.GetTokenCredential()
	require.NoError(t, err)
	assert.NotNil(t, spt)
}

func TestFallbackToMSIbutAzureAuthDisallowed(t *testing.T) {
	t.Setenv("MSI_ENDPOINT", "test")
	defer os.Unsetenv("MSI_ENDPOINT")
	settings, err := NewEnvironmentSettings(
		map[string]string{
			"azureClientId":    fakeClientID,
			"vaultName":        "vaultName",
			"azureAuthMethods": "None",
		},
	)
	require.NoError(t, err)

	_, err = settings.GetTokenCredential()
	require.Error(t, err)
	require.ErrorContains(t, err, "all Azure auth methods have been disabled")
}

func TestFallbackToMSIandInAllowedList(t *testing.T) {
	t.Setenv("MSI_ENDPOINT", "test")
	defer os.Unsetenv("MSI_ENDPOINT")
	settings, err := NewEnvironmentSettings(
		map[string]string{
			"azureClientId":    fakeClientID,
			"vaultName":        "vaultName",
			"azureAuthMethods": "clientcredentials,clientcertificate,workloadidentity,managedIdentity",
		},
	)
	require.NoError(t, err)

	testCertConfig := settings.GetMSI()
	assert.NotNil(t, testCertConfig)

	spt, err := settings.GetTokenCredential()
	require.NoError(t, err)
	assert.NotNil(t, spt)
}

func TestFallbackToMSIandNotInAllowedList(t *testing.T) {
	t.Setenv("MSI_ENDPOINT", "test")
	defer os.Unsetenv("MSI_ENDPOINT")
	settings, err := NewEnvironmentSettings(
		map[string]string{
			"azureClientId":    fakeClientID,
			"vaultName":        "vaultName",
			"azureAuthMethods": "clientcredentials,clientcertificate,workloadidentity",
		},
	)
	require.NoError(t, err)

	_, err = settings.GetTokenCredential()
	require.Error(t, err)
	require.ErrorContains(t, err, "no suitable token provider for Azure AD")
}

func TestFallbackToMSIandInvalidAuthMethod(t *testing.T) {
	t.Setenv("MSI_ENDPOINT", "test")
	defer os.Unsetenv("MSI_ENDPOINT")
	settings, err := NewEnvironmentSettings(
		map[string]string{
			"azureClientId":    fakeClientID,
			"vaultName":        "vaultName",
			"azureAuthMethods": "clientcredentials,clientcertificate,workloadidentity,managedIdentity,cli,SUPERAUTH",
		},
	)
	require.NoError(t, err)

	testCertConfig := settings.GetMSI()
	require.NotNil(t, testCertConfig)

	_, err = settings.GetTokenCredential()
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid Azure auth method: superauth")
}

func TestAuthorizorWithMSIAndUserAssignedID(t *testing.T) {
	t.Setenv("MSI_ENDPOINT", "test")
	defer os.Unsetenv("MSI_ENDPOINT")
	settings, err := NewEnvironmentSettings(
		map[string]string{
			"azureClientId": fakeClientID,
			"vaultName":     "vaultName",
		},
	)
	require.NoError(t, err)

	testCertConfig := settings.GetMSI()
	assert.NotNil(t, testCertConfig)

	spt, err := settings.GetTokenCredential()
	require.NoError(t, err)
	assert.NotNil(t, spt)
}

func getTestCert() []byte {
	certBytes, _ := base64.StdEncoding.DecodeString(testCert)

	return certBytes
}

func setWorkloadIdentityEnvironment(t *testing.T, authorityHost string) {
	t.Helper()

	tokenFile := filepath.Join(t.TempDir(), "federated-token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("federated-token"), 0o600))

	t.Setenv("AZURE_AUTHORITY_HOST", authorityHost)
	t.Setenv("AZURE_CLIENT_ID", fakeClientID)
	t.Setenv("AZURE_FEDERATED_TOKEN_FILE", tokenFile)
	t.Setenv("AZURE_TENANT_ID", fakeTenantID)
}

func TestFallbackToCLI(t *testing.T) {
	settings, err := NewEnvironmentSettings(
		map[string]string{
			"vaultName": "vaultName",
		},
	)
	require.NoError(t, err)

	// check whether this test can be run (i.e. Azure CLI is installed and logged in)

	runTest := false
	cred, credErr := azidentity.NewAzureCLICredential(nil)
	if credErr == nil {
		ctx, cancelFunc := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancelFunc()
		token, err := cred.GetToken(ctx, policy.TokenRequestOptions{})
		if err == nil && token.Token != "" {
			runTest = true
		}
	}

	if runTest {
		spt, err := settings.GetTokenCredential()
		require.NoError(t, err)

		token, _ := spt.GetToken(t.Context(), policy.TokenRequestOptions{})
		assert.NotNil(t, token)
		assert.NotEmpty(t, token.Token)
	} else {
		t.Skip("Skipping test as Azure CLI is not installed or logged in. This test would fall through to MSI which is not available in the test environment.")
	}
}
