/*
Copyright 2024 The Dapr Authors
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
package googleai

import (
	"context"
	"net/http"
	"reflect"

	"github.com/tmc/langchaingo/llms/openai"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google/externalaccount"

	spiffeauth "github.com/dapr/components-contrib/common/authentication/spiffe"
	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/langchaingokit"
	"github.com/dapr/components-contrib/metadata"
	kitspiffe "github.com/dapr/kit/crypto/spiffe"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"
)

type GoogleAI struct {
	langchaingokit.LLM

	logger logger.Logger
}

func NewGoogleAI(logger logger.Logger) conversation.Conversation {
	g := &GoogleAI{
		logger: logger,
		LLM:    langchaingokit.New(logger),
	}

	return g
}

const (
	// endpoint from https://ai.google.dev/gemini-api/docs/openai
	defaultEndpoint = "https://generativelanguage.googleapis.com/v1beta/openai/"

	cloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"
)

// gcpSTSTokenURL is a variable so tests can point it at a fake.
var gcpSTSTokenURL = "https://sts.googleapis.com/v1/token" //nolint:gosec

func (g *GoogleAI) Init(ctx context.Context, meta conversation.Metadata) error {
	md := GoogleAILangchainMetadata{}
	err := kmeta.DecodeMetadata(meta.Properties, &md)
	if err != nil {
		return err
	}

	// Resolve model via central helper (uses metadata, then env var, then default)
	model := conversation.GetGoogleAIModel(md.Model)
	key, _ := meta.GetProperty("key")

	endpoint := defaultEndpoint
	if md.Endpoint != "" {
		endpoint = md.Endpoint
	}

	var opts []openai.Option
	if md.WorkloadIdentityProvider == "" {
		opts = conversation.BuildOpenAIClientOptions(model, key, endpoint)
	} else {
		ts, tsErr := federationTokenSource(ctx, md)
		if tsErr != nil {
			return tsErr
		}
		opts = append(conversation.BuildOpenAIClientOptions(model, conversation.PlaceholderToken, endpoint),
			openai.WithHTTPClient(conversation.BuildBearerHTTPClient(ts)),
		)
	}

	llm, err := openai.New(opts...)
	if err != nil {
		return err
	}

	g.Model = llm
	g.SetModel(model)
	g.SetDefaultMaxTokens(md.MaxTokens)
	// Gemini's OpenAI-compatible endpoint documents the legacy max_tokens
	// field, not max_completion_tokens; use it so the cap is honored.
	g.SetPostCallOptions(openai.WithLegacyMaxTokensField())

	if md.ResponseCacheTTL != nil {
		cachedModel, cacheErr := conversation.CacheResponses(ctx, md.ResponseCacheTTL, g.Model)
		if cacheErr != nil {
			return cacheErr
		}

		g.Model = cachedModel
	}
	return nil
}

// federationTokenSource exchanges JWT SVIDs for Google access tokens with the
// Google Cloud Security Token Service.
func federationTokenSource(ctx context.Context, md GoogleAILangchainMetadata) (oauth2.TokenSource, error) {
	minter, err := spiffeauth.MinterFrom(ctx)
	if err != nil {
		return nil, err
	}

	cfg := externalaccount.Config{ //nolint:gosec
		Audience:             md.WorkloadIdentityProvider,
		SubjectTokenType:     "urn:ietf:params:oauth:token-type:jwt",
		TokenURL:             gcpSTSTokenURL,
		SubjectTokenSupplier: svidSupplier{minter: minter},
		Scopes:               []string{cloudPlatformScope},
	}
	if md.ServiceAccountEmail != "" {
		cfg.ServiceAccountImpersonationURL = "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/" + md.ServiceAccountEmail + ":generateAccessToken"
	}

	// The token source keeps the given context for every refresh, so it must
	// not be the Init context, which is cancelled once Init returns. The STS
	// and impersonation requests use this client: the impersonation client is
	// built from its transport alone, so the transport carries a timeout too.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = spiffeauth.ExchangeTimeout
	client := &http.Client{Transport: transport, Timeout: spiffeauth.ExchangeTimeout}

	return externalaccount.NewTokenSource(context.WithValue(context.Background(), oauth2.HTTPClient, client), cfg)
}

// svidSupplier supplies a newly minted JWT SVID for every token exchange.
type svidSupplier struct {
	minter kitspiffe.JWTSVIDMinter
}

func (s svidSupplier) SubjectToken(ctx context.Context, opts externalaccount.SupplierOptions) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, spiffeauth.ExchangeTimeout)
	defer cancel()
	return spiffeauth.MintJWT(ctx, s.minter, opts.Audience)
}

func (g *GoogleAI) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := GoogleAILangchainMetadata{}
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (g *GoogleAI) Close() error {
	return nil
}
