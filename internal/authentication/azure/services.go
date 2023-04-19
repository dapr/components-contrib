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

const (
	// Service configuration for Azure SQL. Namespaced with dapr.io
	ServiceAzureSQL cloud.ServiceName = "dapr.io/azuresql"
)

func init() {
	// Register additional services in the SDK's clouds
	cloud.AzureChina.Services[ServiceAzureSQL] = cloud.ServiceConfiguration{
		Audience: "https://database.chinacloudapi.cn",
	}
	cloud.AzureGovernment.Services[ServiceAzureSQL] = cloud.ServiceConfiguration{
		Audience: "https://database.usgovcloudapi.net",
	}
	cloud.AzurePublic.Services[ServiceAzureSQL] = cloud.ServiceConfiguration{
		Audience: "https://database.windows.net",
	}
}
