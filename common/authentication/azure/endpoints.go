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
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
)

type azureService string

var (
	ServiceAzureStorage  azureService = "azurestorage"
	ServiceAzureKeyVault azureService = "azurekeyvault"
)

// EndpointSuffix returns the suffix for the endpoint depending on the cloud used.
func (s EnvironmentSettings) EndpointSuffix(service azureService) string {
	// Note we are panicking in case the service or cloud doesn't exist, because that should be a development-time error only.
	switch s.Cloud {
	case nil, &cloud.AzurePublic:
		switch service {
		case ServiceAzureStorage:
			return "core.windows.net"
		case ServiceAzureKeyVault:
			return "vault.azure.net"
		}
		panic("Invalid service: " + service)
	case &cloud.AzureChina:
		switch service {
		case ServiceAzureStorage:
			return "core.chinacloudapi.cn"
		case ServiceAzureKeyVault:
			return "vault.azure.cn"
		}
		panic("Invalid service: " + service)
	case &cloud.AzureGovernment:
		switch service {
		case ServiceAzureStorage:
			return "core.usgovcloudapi.net"
		case ServiceAzureKeyVault:
			return "vault.usgovcloudapi.net"
		}
		panic("Invalid service: " + service)
	}
	panic("Invalid cloud environment")
}
