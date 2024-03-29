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

// MetadataKeys contains keys for all metadata properties.
var MetadataKeys = map[string][]string{ //nolint:gochecknoglobals
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
	// Allowed values (case-insensitive): AzurePublicCloud/AzurePublic, AzureChinaCloud/AzureChina, AzureUSGovernmentCloud/AzureUSGovernment
	"AzureEnvironment": {"azureEnvironment", "azureCloud"},
	// Identifier for the Azure authentication methods to try (in order), comma-separated
	// Allowed values (case-insensitive): ClientCredentials, creds, ClientCertificate, cert, WorkloadIdentity, wi, ManagedIdentity, mi, CommandLineInterface, cli, None
	"AzureAuthMethods": {"azureAuthMethods", "azureADAuthMethods", "entraIDAuthMethods", "microsoftEntraIDAuthMethods"},

	// Metadata keys for storage components

	// Storage account name.
	"StorageAccountName": {"accountName", "storageAccount", "storageAccountName"},
	// Account key for storage.
	"StorageAccountKey": {"accountKey", "accessKey", "storageAccessKey", "storageAccountKey"},
	// Blob storage container name
	"StorageContainerName": {"containerName", "container", "storageAccountContainer"},
	// Queue storage queue name
	"StorageQueueName": {"queueName", "queue", "storageAccountQueue"},
	// Table storage table name
	"StorageTableName": {"tableName", "table", "storageAccountTable"},
	// Endpoint for storage
	"StorageEndpoint": {"endpoint", "storageEndpoint", "storageAccountEndpoint", "queueEndpointUrl"},
}

// Default Azure environment.
const DefaultAzureEnvironment = "AzurePublic"
