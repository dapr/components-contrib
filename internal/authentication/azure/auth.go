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
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"golang.org/x/crypto/pkcs12"

	"github.com/dapr/components-contrib/metadata"
)

// EnvironmentSettings hold settings to authenticate with Azure.
type EnvironmentSettings struct {
	Metadata map[string]string
	Cloud    *cloud.Configuration
}

// timeoutWrapper prevents a potentially very long timeout when managed identity or CLI credential aren't available
type timeoutWrapper struct {
	cred azcore.TokenCredential
	// timeout applies to all auth attempts until one doesn't time out
	timeout    time.Duration
	authmethod string
}

// GetToken wraps Token acquisition attempts with a short timeout because managed identity or CLI credential
// may not be available and connecting to IMDS can take several minutes to time out.
func (w *timeoutWrapper) GetToken(ctx context.Context, opts policy.TokenRequestOptions) (azcore.AccessToken, error) {
	var tk azcore.AccessToken
	var err error
	// no need to synchronize around this value because it's written only within ChainedTokenCredential's critical section
	if w.timeout > 0 {
		c, cancel := context.WithTimeout(ctx, w.timeout)
		defer cancel()
		tk, err = w.cred.GetToken(c, opts)
		if ce := c.Err(); errors.Is(ce, context.DeadlineExceeded) {
			err = azidentity.NewCredentialUnavailableError(w.authmethod)
		} else {
			// some managed identity implementation is available, so don't apply the timeout to future calls
			w.timeout = 0
		}
	} else {
		tk, err = w.cred.GetToken(ctx, opts)
	}
	return tk, err
}

// NewEnvironmentSettings returns a new EnvironmentSettings configured for a given Azure resource.
func NewEnvironmentSettings(md map[string]string) (EnvironmentSettings, error) {
	es := EnvironmentSettings{
		Metadata: md,
	}
	azureCloud, err := es.GetAzureEnvironment()
	if err != nil {
		return es, err
	}
	es.Cloud = azureCloud
	return es, nil
}

// GetAzureEnvironment returns the Azure environment for a given name.
func (s EnvironmentSettings) GetAzureEnvironment() (*cloud.Configuration, error) {
	envName, _ := s.GetEnvironment("AzureEnvironment")
	switch strings.ToLower(envName) {
	case "", "azurepubliccloud", "azurepublic": // Default value if envName is empty
		return &cloud.AzurePublic, nil
	case "azurechinacloud", "azurechina":
		return &cloud.AzureChina, nil
	case "azureusgovernmentcloud", "azureusgovernment":
		return &cloud.AzureGovernment, nil
	default:
		return nil, fmt.Errorf("invalid Azure cloud: %v", envName)
	}
}

// GetTokenCredential returns an azcore.TokenCredential retrieved from, in order:
// 1. Client credentials
// 2. Client certificate
// 3. Workload identity
// 4. MSI with a timeout of 1 second
// 5. Azure CLI
// 6. Retry MSI without timeout
//
// This order and timeout (with the exception of the additional step 5) matches the DefaultAzureCredential.
func (s EnvironmentSettings) GetTokenCredential() (azcore.TokenCredential, error) {
	// Create a chain
	var creds []azcore.TokenCredential
	errs := make([]error, 0, 3)

	// 1. Client credentials
	if c, e := s.GetClientCredentials(); e == nil {
		cred, err := c.GetTokenCredential()
		if err == nil {
			creds = append(creds, cred)
		} else {
			errs = append(errs, err)
		}
	}

	// 2. Client certificate
	if c, e := s.GetClientCert(); e == nil {
		cred, err := c.GetTokenCredential()
		if err == nil {
			creds = append(creds, cred)
		} else {
			errs = append(errs, err)
		}
	}

	// 3. Workload identity
	// workload identity requires values for AZURE_AUTHORITY_HOST, AZURE_CLIENT_ID, AZURE_FEDERATED_TOKEN_FILE, AZURE_TENANT_ID
	// The workload identity mutating admissions webhook in Kubernetes injects these values into the pod.
	// These environment variables are read using the default WorkloadIdentityCredentialOptions

	workloadCred, err := azidentity.NewWorkloadIdentityCredential(nil)
	if err == nil {
		creds = append(creds, workloadCred)
	} else {
		errs = append(errs, err)
	}

	// 4. MSI with timeout of 1 second (same as DefaultAzureCredential)
	var msiCred *azcore.TokenCredential
	{
		c := s.GetMSI()
		msiCred, err := c.GetTokenCredential()
		if err == nil {
			creds = append(creds, &timeoutWrapper{cred: msiCred, authmethod: "managed identity", timeout: 1 * time.Second})
		} else {
			errs = append(errs, err)
		}
	}

	// 5. AzureCLICredential
	{
		cred, credErr := azidentity.NewAzureCLICredential(nil)
		if credErr == nil {
			creds = append(creds, &timeoutWrapper{cred: cred, authmethod: "Azure CLI", timeout: 5 * time.Second})
		} else {
			errs = append(errs, credErr)
		}
	}

	// 6. Retry MSI without timeout
	if msiCred != nil {
		creds = append(creds, *msiCred)
	}

	if len(creds) == 0 {
		return nil, fmt.Errorf("no suitable token provider for Azure AD; errors: %w", errors.Join(errs...))
	}
	// The ChainedTokenCredential executes the auth methods (with their given timeouts) in order. No execution occurs before this method.
	// As such there is no option to run the auth methods in parallel.
	// It would be ideal if the Azure SDK for Go team could support a parallel execution option.
	return azidentity.NewChainedTokenCredential(creds, nil)
}

// GetClientCredentials creates a config object from the available client credentials.
// An error is returned if no certificate credentials are available.
func (s EnvironmentSettings) GetClientCredentials() (config CredentialsConfig, err error) {
	azureCloud, err := s.GetAzureEnvironment()
	if err != nil {
		return config, err
	}

	config.ClientID, _ = s.GetEnvironment("ClientID")
	config.ClientSecret, _ = s.GetEnvironment("ClientSecret")
	config.TenantID, _ = s.GetEnvironment("TenantID")

	if config.ClientID == "" || config.ClientSecret == "" || config.TenantID == "" {
		return config, errors.New("parameters clientId, clientSecret, and tenantId must all be present")
	}

	config.AzureCloud = azureCloud

	return config, nil
}

// GetClientCert creates a config object from the available certificate credentials.
// An error is returned if no certificate credentials are available.
func (s EnvironmentSettings) GetClientCert() (config CertConfig, err error) {
	azureCloud, err := s.GetAzureEnvironment()
	if err != nil {
		return config, err
	}

	config.CertificatePath, _ = s.GetEnvironment("CertificateFile")
	config.CertificatePassword, _ = s.GetEnvironment("CertificatePassword")
	certBytes, _ := s.GetEnvironment("Certificate")
	config.ClientID, _ = s.GetEnvironment("ClientID")
	config.TenantID, _ = s.GetEnvironment("TenantID")

	if config.CertificatePath == "" && certBytes == "" {
		return config, errors.New("missing client certificate")
	}

	config.CertificateData = []byte(certBytes)
	config.AzureCloud = azureCloud

	return config, nil
}

// GetMSI creates a MSI config object from the available client ID.
func (s EnvironmentSettings) GetMSI() (config MSIConfig) {
	// This is optional and it's ok if value is empty
	config.ClientID, _ = s.GetEnvironment("ClientID")

	return config
}

// CredentialsConfig provides the options to get a bearer authorizer from client credentials.
type CredentialsConfig struct {
	ClientID     string
	ClientSecret string
	TenantID     string
	AzureCloud   *cloud.Configuration
}

// GetTokenCredential returns the azcore.TokenCredential object from the credentials.
func (c CredentialsConfig) GetTokenCredential() (token azcore.TokenCredential, err error) {
	var opts *azidentity.ClientSecretCredentialOptions
	if c.AzureCloud != nil {
		opts = &azidentity.ClientSecretCredentialOptions{
			ClientOptions: azcore.ClientOptions{
				Cloud: *c.AzureCloud,
			},
		}
	}
	return azidentity.NewClientSecretCredential(c.TenantID, c.ClientID, c.ClientSecret, opts)
}

// CertConfig provides the options to get a bearer authorizer from a client certificate.
type CertConfig struct {
	ClientID            string
	CertificatePath     string
	CertificatePassword string
	TenantID            string
	CertificateData     []byte
	AzureCloud          *cloud.Configuration
}

// GetTokenCredential returns the azcore.TokenCredential object from client certificate.
func (c CertConfig) GetTokenCredential() (token azcore.TokenCredential, err error) {
	// Certificate data - may be empty here
	data := c.CertificateData

	// If we have a certificate path, load it
	if c.CertificatePath != "" {
		var errB error
		data, errB = os.ReadFile(c.CertificatePath)
		if errB != nil {
			return nil, fmt.Errorf("failed to read the certificate file (%s): %v", c.CertificatePath, errB)
		}
	}
	if len(data) == 0 {
		return nil, errors.New("certificate is not given")
	}

	// Decode the certificate
	cert, key, err := c.decodeCertificate(data, c.CertificatePassword)
	if err != nil || cert == nil {
		return nil, fmt.Errorf("failed to decode pkcs12 certificate while creating spt: %v", err)
	}

	// Create the azcore.TokenCredential object
	certs := []*x509.Certificate{cert}
	var opts *azidentity.ClientCertificateCredentialOptions
	if c.AzureCloud != nil {
		opts = &azidentity.ClientCertificateCredentialOptions{
			ClientOptions: azcore.ClientOptions{
				Cloud: *c.AzureCloud,
			},
		}
	}
	return azidentity.NewClientCertificateCredential(c.TenantID, c.ClientID, certs, key, opts)
}

// Decode a certificate, either as a PKCS#12 (PFX) bundle, or as a single file with both certificate and key encoded in PEM blocks.
// The password is only used for PFX (and could be empty).
func (c CertConfig) decodeCertificate(data []byte, password string) (certificate *x509.Certificate, privateKey *rsa.PrivateKey, err error) {
	// First, try to decode the certificate as PKCS#12
	certificate, privateKey, err = c.decodePkcs12(data, password)
	if err == nil && certificate != nil {
		return certificate, privateKey, nil
	}

	// If it failed, try decoding as PEM
	certificate, privateKey, err = c.decodePEM(data)
	if err == nil && certificate != nil {
		return certificate, privateKey, nil
	}

	return nil, nil, errors.New("certificate is not valid")
}

func (c CertConfig) decodePkcs12(pkcs []byte, password string) (*x509.Certificate, *rsa.PrivateKey, error) {
	privateKey, certificate, err := pkcs12.Decode(pkcs, password)
	if err != nil {
		return nil, nil, err
	}

	rsaPrivateKey, isRsaKey := privateKey.(*rsa.PrivateKey)
	if !isRsaKey {
		return nil, nil, errors.New("PKCS#12 certificate must contain an RSA private key")
	}

	return certificate, rsaPrivateKey, nil
}

func (c CertConfig) decodePEM(data []byte) (certificate *x509.Certificate, privateKey *rsa.PrivateKey, err error) {
	// We should have 2 PEM blocks: a certificate and a key
	var (
		block     *pem.Block
		parsedKey any
		ok        bool
	)
	for i := 0; i < 2; i++ {
		block, data = pem.Decode(data)
		if block == nil {
			break
		}

		switch block.Type {
		case "CERTIFICATE":
			// If we already have a certificate decoded, return an error
			if certificate != nil {
				return nil, nil, errors.New("invalid certificate")
			}
			certificate, err = x509.ParseCertificate(block.Bytes)
			if err != nil {
				return nil, nil, err
			}
		case "PRIVATE KEY": // PKCS#8
			// If we already have a key decoded, return an error
			if privateKey != nil {
				return nil, nil, errors.New("invalid certificate")
			}
			parsedKey, err = x509.ParsePKCS8PrivateKey(block.Bytes)
			if err != nil {
				return nil, nil, err
			}
			privateKey, ok = parsedKey.(*rsa.PrivateKey)
			if !ok || privateKey == nil {
				return nil, nil, errors.New("certificate must contain an RSA private key")
			}
		case "RSA PRIVATE KEY": // PKCS#1
			// If we already have a key decoded, return an error
			if privateKey != nil {
				return nil, nil, errors.New("invalid certificate")
			}
			parsedKey, err = x509.ParsePKCS1PrivateKey(block.Bytes)
			if err != nil {
				return nil, nil, err
			}
			privateKey, ok = parsedKey.(*rsa.PrivateKey)
			if !ok || privateKey == nil {
				return nil, nil, errors.New("certificate must contain an RSA private key")
			}
		}
	}

	// We should have both a private key and a certificate
	if privateKey == nil || certificate == nil {
		return nil, nil, errors.New("invalid certificate")
	}
	return certificate, privateKey, nil
}

// MSIConfig provides the options to get a bearer authorizer through MSI.
type MSIConfig struct {
	ClientID string
}

// GetTokenCredential returns the azcore.TokenCredential object from MSI.
func (c MSIConfig) GetTokenCredential() (token azcore.TokenCredential, err error) {
	opts := &azidentity.ManagedIdentityCredentialOptions{}
	if c.ClientID != "" {
		opts.ID = azidentity.ClientID(c.ClientID)
	}
	return azidentity.NewManagedIdentityCredential(opts)
}

// GetAzureEnvironment returns the Azure environment for a given name, supporting aliases too.
func (s EnvironmentSettings) GetEnvironment(key string) (val string, ok bool) {
	return metadata.GetMetadataProperty(s.Metadata, MetadataKeys[key]...)
}
