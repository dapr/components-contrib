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

package auth

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/common/authentication/spiffe/spiffetest"
	spiffecontext "github.com/dapr/kit/crypto/spiffe/context"
	"github.com/dapr/kit/logger"
)

func TestWebIdentity(t *testing.T) {
	var form url.Values
	sts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.NoError(t, r.ParseForm())
		form = r.PostForm
		w.Header().Set("Content-Type", "text/xml")
		_, _ = io.WriteString(w, `<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <AccessKeyId>AKIATEST</AccessKeyId>
      <SecretAccessKey>secret</SecretAccessKey>
      <SessionToken>session</SessionToken>
      <Expiration>2099-01-01T00:00:00Z</Expiration>
    </Credentials>
  </AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>`)
	}))
	t.Cleanup(sts.Close)

	opts := Options{
		Logger:                logger.NewLogger("test"),
		AssumeRoleArn:         "arn:aws:iam::123456789012:role/dapr",
		AssumeRoleSessionName: "my-session",
		WebIdentityAudience:   "sts.amazonaws.com",
	}
	configOpts := []func(*config.LoadOptions) error{
		config.WithRegion("us-east-1"),
		config.WithBaseEndpoint(sts.URL),
	}

	t.Run("assumes the role with a newly minted JWT SVID", func(t *testing.T) {
		minter := new(spiffetest.Minter)
		provider, err := NewCredentialProvider(spiffecontext.WithJWTMinter(t.Context(), minter), opts, configOpts)
		require.NoError(t, err)
		assert.Equal(t, WebIdentityProviderType, provider.Type())

		creds, err := provider.Retrieve(t.Context())
		require.NoError(t, err)
		assert.Equal(t, "AKIATEST", creds.AccessKeyID)
		assert.Equal(t, "session", creds.SessionToken)

		require.Len(t, minter.Tokens(), 1)
		assert.Equal(t, "AssumeRoleWithWebIdentity", form.Get("Action"))
		assert.Equal(t, "arn:aws:iam::123456789012:role/dapr", form.Get("RoleArn"))
		assert.Equal(t, "my-session", form.Get("RoleSessionName"))
		assert.Equal(t, minter.Tokens()[0], form.Get("WebIdentityToken"))
	})

	t.Run("requires the Dapr identity", func(t *testing.T) {
		_, err := NewCredentialProvider(t.Context(), opts, configOpts)
		require.ErrorContains(t, err, "enable mTLS")
	})

	t.Run("assumeRoleArn alone keeps using AssumeRole", func(t *testing.T) {
		staticOpts := opts
		staticOpts.WebIdentityAudience = ""
		provider, err := NewCredentialProvider(spiffecontext.WithJWTMinter(t.Context(), new(spiffetest.Minter)), staticOpts, configOpts)
		require.NoError(t, err)
		assert.Equal(t, StaticProviderTypeAssumeRole, provider.Type())
	})
}
