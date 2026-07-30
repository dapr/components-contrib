/*
Copyright 2026 The Dapr Authors
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
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubCredential is a TokenCredential that always succeeds, and records whether it was invoked.
type stubCredential struct {
	called bool
	token  string
}

func (c *stubCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	c.called = true
	return azcore.AccessToken{Token: c.token, ExpiresOn: time.Now().Add(time.Hour)}, nil
}

func TestSpiffeWorkloadIdentityGetTokenCredential(t *testing.T) {
	t.Run("requires clientId and tenantId", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			config SpiffeWorkloadIdentityConfig
		}{
			{"both missing", SpiffeWorkloadIdentityConfig{}},
			{"missing clientId", SpiffeWorkloadIdentityConfig{TenantID: fakeTenantID}},
			{"missing tenantId", SpiffeWorkloadIdentityConfig{ClientID: fakeClientID}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, err := tc.config.GetTokenCredential()
				require.Error(t, err)
			})
		}
	})

	t.Run("returns a credential when clientId and tenantId are present", func(t *testing.T) {
		cred, err := newTestSpiffeCredential()
		require.NoError(t, err)
		assert.NotNil(t, cred)
	})
}

// TestSpiffeCredentialUnavailableWithoutJWTSource asserts that the credential reports itself as
// unavailable, rather than failing fatally, when SPIFFE workload identity is not configured.
func TestSpiffeCredentialUnavailableWithoutJWTSource(t *testing.T) {
	cred, err := newTestSpiffeCredential()
	require.NoError(t, err)

	// The context carries no JWT SVID source.
	_, err = cred.GetToken(context.Background(), policy.TokenRequestOptions{
		Scopes: []string{"https://vault.azure.net/.default"},
	})
	require.Error(t, err)

	// azidentity signals "try the next credential" with credentialUnavailableError, whose type is
	// not exported. NewCredentialUnavailableError produces the same type, so a chain built from
	// only this credential surfaces the unavailable error verbatim rather than wrapping it in an
	// AuthenticationFailedError.
	var authErr *azidentity.AuthenticationFailedError
	require.NotErrorAs(t, err, &authErr,
		"a fatal AuthenticationFailedError halts a ChainedTokenCredential")
}

// TestSpiffeCredentialChainFallsThrough is the regression test for the credential chain halting at
// the SPIFFE step: with no JWT SVID source in the context, the chain must continue to the next
// credential instead of aborting.
func TestSpiffeCredentialChainFallsThrough(t *testing.T) {
	spiffeCred, err := newTestSpiffeCredential()
	require.NoError(t, err)

	next := &stubCredential{token: "token-from-next-credential"}

	chain, err := azidentity.NewChainedTokenCredential(
		[]azcore.TokenCredential{spiffeCred, next}, nil,
	)
	require.NoError(t, err)

	token, err := chain.GetToken(context.Background(), policy.TokenRequestOptions{
		Scopes: []string{"https://vault.azure.net/.default"},
	})
	require.NoError(t, err)

	assert.True(t, next.called, "the chain must continue past the SPIFFE credential")
	assert.Equal(t, "token-from-next-credential", token.Token)
}

func newTestSpiffeCredential() (azcore.TokenCredential, error) {
	return SpiffeWorkloadIdentityConfig{
		TenantID:   fakeTenantID,
		ClientID:   fakeClientID,
		AzureCloud: &cloud.AzurePublic,
	}.GetTokenCredential()
}
