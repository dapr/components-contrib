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
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	spiffeauth "github.com/dapr/components-contrib/common/authentication/spiffe"
	kitspiffe "github.com/dapr/kit/crypto/spiffe"
)

// WebIdentity assumes an IAM role with AssumeRoleWithWebIdentity, using a JWT
// SVID minted by the Dapr runtime as the web identity token.
type WebIdentity struct {
	provider *stscreds.WebIdentityRoleProvider
}

func (w *WebIdentity) Retrieve(ctx context.Context) (aws.Credentials, error) {
	return w.provider.Retrieve(ctx)
}

func (w *WebIdentity) Type() ProviderType {
	return WebIdentityProviderType
}

func newAuthWebIdentity(ctx context.Context, opts Options, configOpts []func(*config.LoadOptions) error) (*WebIdentity, error) {
	minter, err := spiffeauth.MinterFrom(ctx)
	if err != nil {
		return nil, err
	}

	// AssumeRoleWithWebIdentity is not signed, so no credentials are needed to
	// call STS.
	cfg, err := config.LoadDefaultConfig(ctx, append(configOpts, config.WithCredentialsProvider(aws.AnonymousCredentials{}))...)
	if err != nil {
		return nil, err
	}

	provider := stscreds.NewWebIdentityRoleProvider(
		sts.NewFromConfig(cfg),
		opts.AssumeRoleArn,
		jwtSVIDRetriever{minter: minter, audience: opts.WebIdentityAudience},
		func(o *stscreds.WebIdentityRoleOptions) {
			o.RoleSessionName = opts.AssumeRoleSessionName
		},
	)

	return &WebIdentity{provider: provider}, nil
}

// jwtSVIDRetriever mints a new JWT SVID for every AssumeRoleWithWebIdentity
// call. Callers wrap the provider in aws.NewCredentialsCache, so this only
// runs when the role credentials near expiry.
type jwtSVIDRetriever struct {
	minter   kitspiffe.JWTSVIDMinter
	audience string
}

func (r jwtSVIDRetriever) GetIdentityToken() ([]byte, error) {
	// GetIdentityToken has no ctx parameter.
	ctx, cancel := context.WithTimeout(context.Background(), spiffeauth.ExchangeTimeout)
	defer cancel()

	jwt, err := spiffeauth.MintJWT(ctx, r.minter, r.audience)
	if err != nil {
		return nil, err
	}
	return []byte(jwt), nil
}
