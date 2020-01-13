// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package keyvault

import (
	"crypto/rsa"
	"crypto/x509"
	"fmt"

	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/Azure/go-autorest/autorest/azure/auth"
	"golang.org/x/crypto/pkcs12"
)

// CertConfig provides the options to get a bearer authorizer from a client certificate.
type CertConfig struct {
	*auth.ClientCertificateConfig
	CertificateData []byte
}

// GetClientCert creates a config object from the available certificate credentials.
// An error is returned if no certificate credentials are available.
func (k keyvaultSecretStore) GetClientCert() (CertConfig, error) {
	props := k.metadata.Properties
	certFilePath := props[componentSPNCertificateFile]
	certBytes := []byte(props[componentSPNCertificate])
	certPassword := props[componentSPNCertificatePassword]
	clientID := props[componentSPNClientID]
	tenantID := props[componentSPNTenantID]

	if certFilePath == "" && certBytes == nil {
		return CertConfig{}, fmt.Errorf("missing client secret")
	}

	authorizer := NewCertConfig(certFilePath, certBytes, certPassword, clientID, tenantID)

	return authorizer, nil
}

// NewCertConfig creates an ClientAuthorizer object configured to obtain an Authorizer through Client Credentials.
func NewCertConfig(certificatePath string, certificateBytes []byte, certificatePassword string, clientID string, tenantID string) CertConfig {
	return CertConfig{
		&auth.ClientCertificateConfig{
			CertificatePath:     certificatePath,
			CertificatePassword: certificatePassword,
			ClientID:            clientID,
			TenantID:            tenantID,
			Resource:            azure.PublicCloud.ResourceIdentifiers.KeyVault,
			AADEndpoint:         azure.PublicCloud.ActiveDirectoryEndpoint,
		},
		certificateBytes,
	}
}

// Authorizer gets an authorizer object from client certificate.
func (c CertConfig) Authorizer() (autorest.Authorizer, error) {
	if c.ClientCertificateConfig.CertificatePath != "" {
		// in standalone mode, component yaml will pass cert path
		return c.ClientCertificateConfig.Authorizer()
	} else if len(c.CertificateData) > 0 {
		// in kubernetes mode, runtime will get the secret from K8S secret store and pass byte array
		spToken, err := c.ServicePrincipalTokenByCertBytes()
		if err != nil {
			return nil, fmt.Errorf("failed to get oauth token from certificate auth: %v", err)
		}
		return autorest.NewBearerAuthorizer(spToken), nil
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

// MSIConfig provides the options to get a bearer authorizer through MSI.
type MSIConfig struct {
	Resource string
	ClientID string
}

// NewMSIConfig creates an MSIConfig object configured to obtain an Authorizer through MSI.
func NewMSIConfig() MSIConfig {
	return MSIConfig{
		Resource: azure.PublicCloud.ResourceManagerEndpoint,
	}
}

// GetMSI creates a MSI config object from the available client ID.
func (k keyvaultSecretStore) GetMSI() MSIConfig {
	props := k.metadata.Properties
	config := NewMSIConfig()
	config.Resource = azure.PublicCloud.ResourceIdentifiers.KeyVault
	config.ClientID = props[componentSPNClientID]
	return config
}

// Authorizer gets the authorizer from MSI.
func (mc MSIConfig) Authorizer() (autorest.Authorizer, error) {
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

	return autorest.NewBearerAuthorizer(spToken), nil
}

// GetAuthorizer creates an Authorizer configured from environment variables in the order:
// 1. Client certificate
// 2. MSI
func (k keyvaultSecretStore) GetAuthorizer() (autorest.Authorizer, error) {
	k.vaultName = k.metadata.Properties[componentVaultName]
	// 1. Client Certificate
	if c, e := k.GetClientCert(); e == nil {
		return c.Authorizer()
	}

	// 2. MSI
	return k.GetMSI().Authorizer()
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
