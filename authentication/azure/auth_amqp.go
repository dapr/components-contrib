// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

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
