// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package azure

// Keys for all metadata properties
const (
	CertificateKey              = "CERTIFICATE"
	CertificateKeyAlias         = "spnCertificate"
	CertificateFileKey          = "CERTIFICATE_FILE"
	CertificateFileKeyAlias     = "spnCertificateFile"
	CertificatePasswordKey      = "CERTIFICATE_PASSWORD"
	CertificatePasswordKeyAlias = "spnCertificatePassword"
	ClientIDKey                 = "AZURE_CLIENT_ID"
	ClientIDKeyAlias            = "spnClientId"
	ClientSecretKey             = "AZURE_CLIENT_SECRET" // nolint: gosec
	ClientSecretKeyAlias        = "spnClientSecret"
	TenantIDKey                 = "AZURE_TENANT_ID"
	TenantIDKeyAlias            = "spnTenantId"
	// Identifier for the Azure environment
	// Allowed values (case-insensitive): AZUREPUBLICCLOUD, AZURECHINACLOUD, AZUREGERMANCLOUD, AZUREUSGOVERNMENTCLOUD
	AzureEnvironmentKey = "AZURE_ENVIRONMENT"
)

// Default Azure environment
const DefaultAzureEnvironment = "AZUREPUBLICCLOUD"

var KeyAliases = map[string]string{ // nolint: gochecknoglobals
	CertificateKey:         CertificateKeyAlias,
	CertificateFileKey:     CertificateFileKeyAlias,
	CertificatePasswordKey: CertificatePasswordKeyAlias,
	ClientIDKey:            ClientIDKeyAlias,
	ClientSecretKey:        ClientSecretKeyAlias,
	TenantIDKey:            TenantIDKeyAlias,
}
