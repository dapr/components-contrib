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
	envName, ok := s.Values[AzureEnvironmentKey]
	if !ok || envName == "" {
		envName = DefaultAzureEnvironment
	}
	env, err := azure.EnvironmentFromName(envName)
	if err != nil {
		return nil, err
	}

	return &env, err
}

// GetAuthorizer creates an Authorizer retrieved from, in order:
// 1. Client credentials
// 2. Client certificate
// 3. MSI
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

	clientID, _ := s.GetEnvironmentValueByKeyAndAlias(ClientIDKey)
	clientSecret, _ := s.GetEnvironmentValueByKeyAndAlias(ClientSecretKey)
	tenantID, _ := s.GetEnvironmentValueByKeyAndAlias(TenantIDKey)

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

	certFilePath, certFilePathPresent := s.GetEnvironmentValueByKeyAndAlias(CertificateFileKey)
	certBytes, certBytesPresent := s.GetEnvironmentValueByKeyAndAlias(CertificateKey)
	certPassword, _ := s.GetEnvironmentValueByKeyAndAlias(CertificatePasswordKey)
	clientID, _ := s.GetEnvironmentValueByKeyAndAlias(ClientIDKey)
	tenantID, _ := s.GetEnvironmentValueByKeyAndAlias(TenantIDKey)

	if !certFilePathPresent && !certBytesPresent {
		return CertConfig{}, fmt.Errorf("missing client certificate")
	}

	authorizer := NewCertConfig(clientID, tenantID, certFilePath, []byte(certBytes), certPassword, s.Resource, azureEnv)

	return authorizer, nil
}

// GetMSI creates a MSI config object from the available client ID.
func (s EnvironmentSettings) GetMSI() MSIConfig {
	config := NewMSIConfig(s.Resource)
	azureEnv, err := s.GetAzureEnvironment()
	if err != nil {
		azureEnv = &azure.PublicCloud
	}
	config.Resource = azureEnv.ResourceIdentifiers.KeyVault
	config.ClientID, _ = s.GetEnvironmentValueByKeyAndAlias(ClientIDKey)

	return config
}

// CredentialsConfig provides the options to get a bearer authorizer from client credentials
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
func (mc MSIConfig) ServicePrincipalToken() (*adal.ServicePrincipalToken, error) {
	msiEndpoint, err := adal.GetMSIEndpoint()
	if err != nil {
		return nil, err
	}

	var spToken *adal.ServicePrincipalToken
	if mc.ClientID == "" {
		spToken, err = adal.NewServicePrincipalTokenFromMSI(msiEndpoint, mc.Resource)
		if err != nil {
			return nil, fmt.Errorf("failed to get oauth token from MSI: %v", err)
		}
	} else {
		spToken, err = adal.NewServicePrincipalTokenFromMSIWithUserAssignedID(msiEndpoint, mc.Resource, mc.ClientID)
		if err != nil {
			return nil, fmt.Errorf("failed to get oauth token from MSI for user assigned identity: %v", err)
		}
	}

	return spToken, nil
}

// GetAzureEnvironment returns the Azure environment for a given name.
func (s EnvironmentSettings) GetEnvironmentValueByKeyAndAlias(key string) (string, bool) {
	if val, ok := s.Values[key]; ok {
		return val, true
	}
	if val, ok := s.Values[KeyAliases[key]]; ok {
		return val, true
	}

	return "", false
}
