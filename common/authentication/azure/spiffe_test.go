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
	"errors"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/spiffe/go-spiffe/v2/svid/jwtsvid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	spiffecontext "github.com/dapr/kit/crypto/spiffe/context"
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

// stubJWTSource is a jwtsvid.Source that is only used to signal that SPIFFE is configured; the
// credential under test does not fetch from it.
type stubJWTSource struct{}

func (stubJWTSource) FetchJWTSVID(context.Context, jwtsvid.Params) (*jwtsvid.SVID, error) {
	return nil, errors.New("not implemented")
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
// unavailable, rather than failing fatally, when SPIFFE workload identity is not configured. The
// distinction is only observable through a chain: ChainedTokenCredential surfaces a
// credentialUnavailableError verbatim but wraps any other error in an AuthenticationFailedError,
// so the assertion fails if the credential returns a plain error. It also asserts that no token
// request is attempted.
func TestSpiffeCredentialUnavailableWithoutJWTSource(t *testing.T) {
	inner := &stubCredential{token: "token-from-inner-credential"}

	chain, err := azidentity.NewChainedTokenCredential(
		[]azcore.TokenCredential{&spiffeCredential{cred: inner}}, nil,
	)
	require.NoError(t, err)

	// The context carries no JWT SVID source.
	_, err = chain.GetToken(context.Background(), policy.TokenRequestOptions{
		Scopes: []string{"https://vault.azure.net/.default"},
	})
	require.Error(t, err)

	var authErr *azidentity.AuthenticationFailedError
	require.NotErrorAs(t, err, &authErr,
		"a fatal AuthenticationFailedError halts a ChainedTokenCredential")
	assert.False(t, inner.called, "the credential must not attempt a token request")
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

// TestSpiffeCredentialDelegatesWithJWTSource asserts that once a JWT SVID source is available the
// credential delegates to the underlying credential rather than reporting itself as unavailable.
func TestSpiffeCredentialDelegatesWithJWTSource(t *testing.T) {
	inner := &stubCredential{token: "token-from-inner-credential"}
	cred := &spiffeCredential{cred: inner}

	ctx := spiffecontext.WithJWT(context.Background(), stubJWTSource{})
	token, err := cred.GetToken(ctx, policy.TokenRequestOptions{
		Scopes: []string{"https://vault.azure.net/.default"},
	})
	require.NoError(t, err)

	assert.True(t, inner.called, "the credential must delegate when a JWT SVID source is present")
	assert.Equal(t, "token-from-inner-credential", token.Token)
}

// TestGetTokenCredentialSpiffeThenManagedIdentity asserts that the explicit multi-method
// configuration from the issue builds a credential chain: azureAuthMethods lists SPIFFE first
// with managed identity as the fallback.
func TestGetTokenCredentialSpiffeThenManagedIdentity(t *testing.T) {
	// Present a managed identity endpoint so building the MSI credential skips the IMDS
	// reachability probe and its network timeout.
	t.Setenv(identityEndpoint, "http://localhost:8081/msi/token")

	settings, err := NewEnvironmentSettings(map[string]string{
		"azureClientId":    fakeClientID,
		"azureTenantId":    fakeTenantID,
		"azureAuthMethods": "spiffeworkloadidentity,managedidentity",
	})
	require.NoError(t, err)

	// Both methods must contribute a credential to the chain.
	var creds []azcore.TokenCredential
	errs := make([]error, 0)
	settings.addProviderByAuthMethodName("spiffeworkloadidentity", &creds, &errs)
	settings.addProviderByAuthMethodName("managedidentity", &creds, &errs)
	require.Empty(t, errs)
	require.Len(t, creds, 2)

	cred, err := settings.GetTokenCredential()
	require.NoError(t, err)
	assert.NotNil(t, cred)
}

func newTestSpiffeCredential() (azcore.TokenCredential, error) {
	return SpiffeWorkloadIdentityConfig{
		TenantID:   fakeTenantID,
		ClientID:   fakeClientID,
		AzureCloud: &cloud.AzurePublic,
	}.GetTokenCredential()
}
