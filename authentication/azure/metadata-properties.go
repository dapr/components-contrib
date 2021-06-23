// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package azure

// Keys for all metadata properties
const (
	CertificateKey         = "spnCertificate"
	CertificateFileKey     = "spnCertificateFile"
	CertificatePasswordKey = "spnCertificatePassword"
	ClientIDKey            = "spnClientId"
	ClientSecretKey        = "spnClientSecret"
	TenantIDKey            = "spnTenantId"
	// Identifier for the Azure environment
	// Allowed values (case-insensitive): AZUREPUBLICCLOUD, AZURECHINACLOUD, AZUREGERMANCLOUD, AZUREUSGOVERNMENTCLOUD
	AzureEnvironmentKey = "azureEnvironment"
)

// Default Azure environment
const DefaultAzureEnvironment = "AZUREPUBLICCLOUD"
