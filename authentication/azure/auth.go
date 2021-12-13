// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package azure

import (
	"crypto/rsa"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/Azure/go-autorest/autorest/azure/auth"
	"golang.org/x/crypto/pkcs12"
)

// NewEnvironmentSettings returns a new EnvironmentSettings configured for a given Azure resource.
func NewEnvironmentSettings(resourceName string, values map[string]string) (EnvironmentSettings, error) {
	es := EnvironmentSettings{
		Values: values,
	}
	azureEnv, err := es.GetAzureEnvironment()
	if err != nil {
		return es, err
	}
	es.AzureEnvironment = azureEnv
	switch resourceName {
	case "azure":
		// Azure Resource Manager (management plane)
		es.Resource = azureEnv.TokenAudience
	case "keyvault":
		// Azure Key Vault (data plane)
		es.Resource = azureEnv.ResourceIdentifiers.KeyVault
	case "storage":
		// Azure Storage (data plane)
		es.Resource = azureEnv.ResourceIdentifiers.Storage
	case "cosmosdb":
		// Azure Cosmos DB (data plane)
		es.Resource = azureEnv.ResourceIdentifiers.CosmosDB
	case "servicebus":
		es.Resource = azureEnv.ResourceIdentifiers.ServiceBus
	default:
		return es, errors.New("invalid resource name: " + resourceName)
	}

	return es, nil
}

// EnvironmentSettings hold settings to authenticate with Azure.
type EnvironmentSettings struct {
	Values           map[string]string
	Resource         string
	AzureEnvironment *azure.Environment
}

// GetAzureEnvironment returns the Azure environment for a given name.
func (s EnvironmentSettings) GetAzureEnvironment() (*azure.Environment, error) {
	envName, ok := s.GetEnvironment("AzureEnvironment")
	if !ok || envName == "" {
		envName = DefaultAzureEnvironment
	}
	env, err := azure.EnvironmentFromName(envName)
	if err != nil {
		return nil, err
	}

	return &env, err
}

// GetTokenCredential returns an azcore.TokenCredential retrieved from, in order:
// 1. Client credentials
// 2. Client certificate
// 3. MSI
// This is used by the newer ("track 2") Azure SDKs.
func (s EnvironmentSettings) GetTokenCredential() (azcore.TokenCredential, error) {
	// Create a chain
	var creds []azcore.TokenCredential
	errMsg := ""

	// 1. Client credentials
	if c, e := s.GetClientCredentials(); e == nil {
		cred, err := c.GetTokenCredential()
		if err == nil {
			creds = append(creds, cred)
		} else {
			errMsg += err.Error() + "\n"
		}
	}

	// 2. Client certificate
	if c, e := s.GetClientCert(); e == nil {
		cred, err := c.GetTokenCredential()
		if err == nil {
			creds = append(creds, cred)
		} else {
			errMsg += err.Error() + "\n"
		}
	}

	// 3. MSI
	{
		c := s.GetMSI()
		cred, err := c.GetTokenCredential()
		if err == nil {
			creds = append(creds, cred)
		} else {
			errMsg += err.Error() + "\n"
		}
	}

	if len(creds) == 0 {
		return nil, fmt.Errorf("no suitable token provider for Azure AD; errors: %v", errMsg)
	}
	return azidentity.NewChainedTokenCredential(creds, nil)
}

// GetAuthorizer creates an Authorizer retrieved from, in order:
// 1. Client credentials
// 2. Client certificate
// 3. MSI
// This is used by the older Azure SDKs.
func (s EnvironmentSettings) GetAuthorizer() (autorest.Authorizer, error) {
	spt, err := s.GetServicePrincipalToken()
	if err != nil {
		return nil, err
	}

	return autorest.NewBearerAuthorizer(spt), nil
}

// GetServicePrincipalToken returns a Service Principal Token retrieved from, in order:
// 1. Client credentials
// 2. Client certificate
// 3. MSI
// This is used by the older Azure SDKs.
func (s EnvironmentSettings) GetServicePrincipalToken() (*adal.ServicePrincipalToken, error) {
	// 1. Client credentials
	if c, e := s.GetClientCredentials(); e == nil {
		return c.ServicePrincipalToken()
	}

	// 2. Client Certificate
	if c, e := s.GetClientCert(); e == nil {
		return c.ServicePrincipalToken()
	}

	// 3. MSI
	return s.GetMSI().ServicePrincipalToken()
}

// GetClientCredentials creates a config object from the available client credentials.
// An error is returned if no certificate credentials are available.
func (s EnvironmentSettings) GetClientCredentials() (CredentialsConfig, error) {
	azureEnv, err := s.GetAzureEnvironment()
	if err != nil {
		return CredentialsConfig{}, err
	}

	clientID, _ := s.GetEnvironment("ClientID")
	clientSecret, _ := s.GetEnvironment("ClientSecret")
	tenantID, _ := s.GetEnvironment("TenantID")

	if clientID == "" || clientSecret == "" || tenantID == "" {
		return CredentialsConfig{}, errors.New("parameters clientId, clientSecret, and tenantId must all be present")
	}

	authorizer := NewCredentialsConfig(clientID, tenantID, clientSecret, s.Resource, azureEnv)

	return authorizer, nil
}

// GetClientCert creates a config object from the available certificate credentials.
// An error is returned if no certificate credentials are available.
func (s EnvironmentSettings) GetClientCert() (CertConfig, error) {
	azureEnv, err := s.GetAzureEnvironment()
	if err != nil {
		return CertConfig{}, err
	}

	certFilePath, certFilePathPresent := s.GetEnvironment("CertificateFile")
	certBytes, certBytesPresent := s.GetEnvironment("Certificate")
	certPassword, _ := s.GetEnvironment("CertificatePassword")
	clientID, _ := s.GetEnvironment("ClientID")
	tenantID, _ := s.GetEnvironment("TenantID")

	if !certFilePathPresent && !certBytesPresent {
		return CertConfig{}, fmt.Errorf("missing client certificate")
	}

	authorizer := NewCertConfig(clientID, tenantID, certFilePath, []byte(certBytes), certPassword, s.Resource, azureEnv)

	return authorizer, nil
}

// GetMSI creates a MSI config object from the available client ID.
func (s EnvironmentSettings) GetMSI() MSIConfig {
	config := NewMSIConfig(s.Resource)
	// This is optional and it's ok if value is empty
	config.ClientID, _ = s.GetEnvironment("ClientID")

	return config
}

// CredentialsConfig provides the options to get a bearer authorizer from client credentials.
type CredentialsConfig struct {
	*auth.ClientCredentialsConfig
}

// NewCredentialsConfig creates an CredentialsConfig object configured to obtain an Authorizer through Client Credentials.
func NewCredentialsConfig(clientID string, tenantID string, clientSecret string, resource string, env *azure.Environment) CredentialsConfig {
	return CredentialsConfig{
		&auth.ClientCredentialsConfig{
			ClientSecret: clientSecret,
			ClientID:     clientID,
			TenantID:     tenantID,
			Resource:     resource,
			AADEndpoint:  env.ActiveDirectoryEndpoint,
		},
	}
}

// ServicePrincipalToken gets a ServicePrincipalToken object from the credentials.
func (c CredentialsConfig) ServicePrincipalToken() (*adal.ServicePrincipalToken, error) {
	oauthConfig, err := adal.NewOAuthConfig(c.AADEndpoint, c.TenantID)
	if err != nil {
		return nil, err
	}

	return adal.NewServicePrincipalToken(*oauthConfig, c.ClientID, c.ClientSecret, c.Resource)
}

// GetTokenCredential returns the azcore.TokenCredential object from the credentials.
func (c CredentialsConfig) GetTokenCredential() (token azcore.TokenCredential, err error) {
	return azidentity.NewClientSecretCredential(c.TenantID, c.ClientID, c.ClientSecret, &azidentity.ClientSecretCredentialOptions{
		AuthorityHost: azidentity.AuthorityHost(c.AADEndpoint),
	})
}

// CertConfig provides the options to get a bearer authorizer from a client certificate.
type CertConfig struct {
	*auth.ClientCertificateConfig
	CertificateData []byte
}

// NewCertConfig creates an CertConfig object configured to obtain an Authorizer through Client Credentials, using a certificate.
func NewCertConfig(clientID string, tenantID string, certificatePath string, certificateBytes []byte, certificatePassword string, resource string, env *azure.Environment) CertConfig {
	return CertConfig{
		&auth.ClientCertificateConfig{
			CertificatePath:     certificatePath,
			CertificatePassword: certificatePassword,
			ClientID:            clientID,
			TenantID:            tenantID,
			Resource:            resource,
			AADEndpoint:         env.ActiveDirectoryEndpoint,
		},
		certificateBytes,
	}
}

// ServicePrincipalToken gets a ServicePrincipalToken object from client certificate.
func (c CertConfig) ServicePrincipalToken() (*adal.ServicePrincipalToken, error) {
	if c.ClientCertificateConfig.CertificatePath != "" {
		// in standalone mode, component yaml will pass cert path
		return c.ClientCertificateConfig.ServicePrincipalToken()
	} else if len(c.CertificateData) > 0 {
		// in kubernetes mode, runtime will get the secret from K8S secret store and pass byte array
		return c.ServicePrincipalTokenByCertBytes()
	}

	return nil, fmt.Errorf("certificate is not given")
}

// ServicePrincipalTokenByCertBytes gets the service principal token by CertificateBytes.
func (c CertConfig) ServicePrincipalTokenByCertBytes() (*adal.ServicePrincipalToken, error) {
	oauthConfig, err := adal.NewOAuthConfig(c.AADEndpoint, c.TenantID)
	if err != nil {
		return nil, err
	}

	certificate, rsaPrivateKey, err := c.decodePkcs12(c.CertificateData, c.CertificatePassword)
	if err != nil {
		return nil, fmt.Errorf("failed to decode pkcs12 certificate while creating spt: %v", err)
	}

	return adal.NewServicePrincipalTokenFromCertificate(*oauthConfig, c.ClientID, certificate, rsaPrivateKey, c.Resource)
}

// GetTokenCredential returns the azcore.TokenCredential object from client certificate.
func (c CertConfig) GetTokenCredential() (token azcore.TokenCredential, err error) {
	ccc := c.ClientCertificateConfig

	// Certificate data - may be empty here
	data := c.CertificateData

	// If we have a certificate path, load it
	if c.ClientCertificateConfig.CertificatePath != "" {
		var errB error
		data, errB = ioutil.ReadFile(ccc.CertificatePath)
		if errB != nil {
			return nil, fmt.Errorf("failed to read the certificate file (%s): %v", ccc.CertificatePath, errB)
		}
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("certificate is not given")
	}

	// Decode the PKCS#12 certificate
	cert, key, err := c.decodePkcs12(data, c.CertificatePassword)
	if err != nil || cert == nil {
		return nil, fmt.Errorf("failed to decode pkcs12 certificate while creating spt: %v", err)
	}

	// Create the azcore.TokenCredential object
	certs := []*x509.Certificate{cert}
	opts := &azidentity.ClientCertificateCredentialOptions{
		AuthorityHost: azidentity.AuthorityHost(c.AADEndpoint),
	}
	return azidentity.NewClientCertificateCredential(c.TenantID, c.ClientID, certs, key, opts)
}

func (c CertConfig) decodePkcs12(pkcs []byte, password string) (*x509.Certificate, *rsa.PrivateKey, error) {
	privateKey, certificate, err := pkcs12.Decode(pkcs, password)
	if err != nil {
		return nil, nil, err
	}

	rsaPrivateKey, isRsaKey := privateKey.(*rsa.PrivateKey)
	if !isRsaKey {
		return nil, nil, fmt.Errorf("PKCS#12 certificate must contain an RSA private key")
	}

	return certificate, rsaPrivateKey, nil
}

// MSIConfig provides the options to get a bearer authorizer through MSI.
type MSIConfig struct {
	Resource string
	ClientID string
}

// NewMSIConfig creates an MSIConfig object configured to obtain an Authorizer through MSI.
func NewMSIConfig(resource string) MSIConfig {
	return MSIConfig{
		Resource: resource,
	}
}

// ServicePrincipalToken gets the ServicePrincipalToken object from MSI.
func (c MSIConfig) ServicePrincipalToken() (*adal.ServicePrincipalToken, error) {
	msiEndpoint, err := adal.GetMSIEndpoint()
	if err != nil {
		return nil, err
	}

	var spToken *adal.ServicePrincipalToken
	if c.ClientID == "" {
		spToken, err = adal.NewServicePrincipalTokenFromMSI(msiEndpoint, c.Resource)
		if err != nil {
			return nil, fmt.Errorf("failed to get oauth token from MSI: %v", err)
		}
	} else {
		spToken, err = adal.NewServicePrincipalTokenFromMSIWithUserAssignedID(msiEndpoint, c.Resource, c.ClientID)
		if err != nil {
			return nil, fmt.Errorf("failed to get oauth token from MSI for user assigned identity: %v", err)
		}
	}

	return spToken, nil
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
func (s EnvironmentSettings) GetEnvironment(key string) (string, bool) {
	var (
		val string
		ok  bool
	)
	for _, k := range MetadataKeys[key] {
		val, ok = s.Values[k]
		if ok {
			return val, true
		}
	}

	return "", false
}
