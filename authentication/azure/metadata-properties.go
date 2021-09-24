// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package azure

// MetadataKeys : Keys for all metadata properties.
var MetadataKeys = map[string][]string{ // nolint: gochecknoglobals
	// clientId, clientSecret, tenantId are supported for backwards-compatibility as they're used by some components, but should be considered deprecated

	// Certificate contains the raw certificate data
	"Certificate": {"azureCertificate", "spnCertificate"},
	// Path to a certificate
	"CertificateFile": {"azureCertificateFile", "spnCertificateFile"},
	// Password for the certificate
	"CertificatePassword": {"azureCertificatePassword", "spnCertificatePassword"},
	// Client ID for the Service Principal
	// The "clientId" alias is supported for backwards-compatibility as it's used by some components, but should be considered deprecated
	"ClientID": {"azureClientId", "spnClientId", "clientId"},
	// Client secret for the Service Principal
	// The "clientSecret" alias is supported for backwards-compatibility as it's used by some components, but should be considered deprecated
	"ClientSecret": {"azureClientSecret", "spnClientSecret", "clientSecret"},
	// Tenant ID for the Service Principal
	// The "tenantId" alias is supported for backwards-compatibility as it's used by some components, but should be considered deprecated
	"TenantID": {"azureTenantId", "spnTenantId", "tenantId"},
	// Identifier for the Azure environment
	// Allowed values (case-insensitive): AZUREPUBLICCLOUD, AZURECHINACLOUD, AZUREGERMANCLOUD, AZUREUSGOVERNMENTCLOUD
	"AzureEnvironment": {"azureEnvironment"},
}

// Default Azure environment.
const DefaultAzureEnvironment = "AZUREPUBLICCLOUD"
