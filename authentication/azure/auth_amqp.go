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

import "github.com/Azure/azure-amqp-common-go/v3/aad"

const (
	AzureServiceBusResourceName string = "servicebus"
)

// GetTokenProvider creates a TokenProvider for AAD retrieved from, in order:
// 1. Client credentials
// 2. Client certificate
// 3. MSI.
func (s EnvironmentSettings) GetAADTokenProvider() (*aad.TokenProvider, error) {
	spt, err := s.GetServicePrincipalToken()
	if err != nil {
		return nil, err
	}

	return aad.NewJWTProvider(aad.JWTProviderWithAADToken(spt), aad.JWTProviderWithAzureEnvironment(s.AzureEnvironment))
}
