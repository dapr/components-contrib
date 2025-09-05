/*
Copyright 2025 The Dapr Authors
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
	"context"
	"errors"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/spiffe/go-spiffe/v2/svid/jwtsvid"

	spiffecontext "github.com/dapr/kit/crypto/spiffe/context"
)

const (
	//nolint:gosec
	AzureADTokenExchangeAudience = "api://AzureADTokenExchange"
)

// SpiffeWorkloadIdentityConfig provides the options to get a bearer authorizer using SPIFFE-based workload identity.
type SpiffeWorkloadIdentityConfig struct {
	TenantID   string
	ClientID   string
	AzureCloud *cloud.Configuration
}

// GetTokenCredential returns the azcore.TokenCredential object using a SPIFFE JWT token.
func (c SpiffeWorkloadIdentityConfig) GetTokenCredential() (azcore.TokenCredential, error) {
	if c.TenantID == "" || c.ClientID == "" {
		return nil, errors.New("parameters clientId and tenantId must be present for SPIFFE workload identity")
	}

	var opts *azidentity.ClientAssertionCredentialOptions
	if c.AzureCloud != nil {
		opts = &azidentity.ClientAssertionCredentialOptions{
			ClientOptions: azcore.ClientOptions{
				Cloud: *c.AzureCloud,
			},
		}
	}

	// Create a token provider function that retrieves the JWT from SPIFFE context
	tokenProvider := func(ctx context.Context) (string, error) {
		tknSource, ok := spiffecontext.JWTFrom(ctx)
		if !ok {
			return "", errors.New("failed to get JWT SVID source from context")
		}
		jwt, err := tknSource.FetchJWTSVID(ctx, jwtsvid.Params{
			Audience: AzureADTokenExchangeAudience,
		})
		if err != nil {
			return "", fmt.Errorf("failed to get JWT SVID: %w", err)
		}
		return jwt.Marshal(), nil
	}

	return azidentity.NewClientAssertionCredential(c.TenantID, c.ClientID, tokenProvider, opts)
}

// GetSpiffeWorkloadIdentity creates a config object from the available SPIFFE workload identity credentials.
// An error is returned if required credentials are not available.
func (s EnvironmentSettings) GetSpiffeWorkloadIdentity() (config SpiffeWorkloadIdentityConfig, err error) {
	azureCloud, err := s.GetAzureEnvironment()
	if err != nil {
		return config, err
	}

	config.ClientID, _ = s.GetEnvironment("ClientID")
	config.TenantID, _ = s.GetEnvironment("TenantID")

	if config.ClientID == "" || config.TenantID == "" {
		return config, errors.New("parameters clientId and tenantId must be present for SPIFFE workload identity")
	}

	config.AzureCloud = azureCloud

	return config, nil
}
