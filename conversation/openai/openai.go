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
package openai

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"golang.org/x/oauth2"

	azauth "github.com/dapr/components-contrib/common/authentication/azure"
	spiffeauth "github.com/dapr/components-contrib/common/authentication/spiffe"
	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/langchaingokit"
	"github.com/dapr/components-contrib/metadata"
	kitspiffe "github.com/dapr/kit/crypto/spiffe"
	spiffecontext "github.com/dapr/kit/crypto/spiffe/context"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"

	"github.com/tmc/langchaingo/llms/openai"
)

type OpenAI struct {
	langchaingokit.LLM

	logger logger.Logger
	md     OpenAILangchainMetadata
}

func NewOpenAI(logger logger.Logger) conversation.Conversation {
	o := &OpenAI{
		logger: logger,
		LLM:    langchaingokit.New(logger),
	}

	return o
}

func (o *OpenAI) buildClientOptions(md OpenAILangchainMetadata) (string, []openai.Option, error) {
	// Resolve model via central helper (uses metadata, then env var, then default)
	var model string
	// we support lowercase and uppercase here
	if strings.EqualFold(md.APIType, "azure") {
		model = conversation.GetAzureOpenAIModel(md.Model)
	} else {
		model = conversation.GetOpenAIModel(md.Model)
	}
	options := conversation.BuildOpenAIClientOptions(model, md.Key, md.Endpoint)

	// apply options specifically for azure openai
	// TODO: in future, there is also an openai.APITypeAzureAD that we can add.
	if strings.EqualFold(md.APIType, "azure") {
		if md.Endpoint == "" || md.APIVersion == "" {
			return "", nil, errors.New("endpoint and apiVersion must be provided when apiType is set to 'azure'")
		}
		options = append(options,
			openai.WithAPIType(openai.APITypeAzure),
			openai.WithAPIVersion(md.APIVersion),

			// apparently this is required for azure openai (but not for openai)
			// https://github.com/tmc/langchaingo/blob/509308ff01c13e662d5613d3aea793fabe18edd2/llms/openai/openaillm_option.go#L78
			openai.WithEmbeddingModel(md.Model),
		)

		// NOTE: This is also an option here.
		// https://github.com/tmc/langchaingo/blob/509308ff01c13e662d5613d3aea793fabe18edd2/llms/openai/openaillm_option.go#L89
		// openai.WithEmbeddingDimentions(),
	}

	return model, options, nil
}

func (o *OpenAI) Init(ctx context.Context, meta conversation.Metadata) error {
	md := OpenAILangchainMetadata{}
	err := kmeta.DecodeMetadata(meta.Properties, &md)
	if err != nil {
		return err
	}
	o.md = md

	if md.UseResponsesAPI && strings.EqualFold(md.APIType, "azure") {
		return errors.New("useResponsesAPI is not supported when apiType is set to 'azure'")
	}

	model, options, err := o.buildClientOptions(md)
	if err != nil {
		return err
	}

	ts, err := federatedTokenSource(ctx, md, meta.Properties)
	if err != nil {
		return err
	}
	var httpClient *http.Client
	if ts != nil {
		httpClient = conversation.BuildBearerHTTPClient(ts)
		options = append(options,
			openai.WithToken(conversation.PlaceholderToken),
			openai.WithHTTPClient(httpClient),
		)
		if strings.EqualFold(md.APIType, "azure") {
			options = append(options, openai.WithAPIType(openai.APITypeAzureAD))
		}
	}

	if md.UseResponsesAPI {
		o.Model, err = newResponsesModel(model, md.Key, md.Endpoint, httpClient)
	} else {
		o.Model, err = openai.New(options...)
	}
	if err != nil {
		return err
	}

	o.SetModel(model)
	o.SetDefaultMaxTokens(md.MaxTokens)
	if md.UseLegacyMaxTokensField {
		o.SetPostCallOptions(openai.WithLegacyMaxTokensField())
	}

	if md.ResponseCacheTTL != nil {
		cachedModel, cacheErr := conversation.CacheResponses(ctx, md.ResponseCacheTTL, o.Model)
		if cacheErr != nil {
			return cacheErr
		}

		o.Model = cachedModel
	}

	return nil
}

const (
	defaultFederationAudience = "https://api.openai.com/v1"
	cognitiveServicesScope    = "https://cognitiveservices.azure.com/.default"
)

// openAITokenURL is a variable so tests can point it at a fake.
var openAITokenURL = "https://auth.openai.com/oauth/token" //nolint:gosec

// federatedTokenSource returns the token source for keyless authentication,
// or nil when the component authenticates with an API key.
func federatedTokenSource(ctx context.Context, md OpenAILangchainMetadata, props map[string]string) (oauth2.TokenSource, error) {
	switch {
	case md.IdentityProviderID != "":
		if strings.EqualFold(md.APIType, "azure") {
			return nil, errors.New("identityProviderId is not supported when apiType is set to 'azure'; use Microsoft Entra ID instead")
		}
		if md.ServiceAccountID == "" {
			return nil, errors.New("serviceAccountId must be provided when identityProviderId is set")
		}

		minter, err := spiffeauth.MinterFrom(ctx)
		if err != nil {
			return nil, err
		}

		audience := md.Audience
		if audience == "" {
			audience = defaultFederationAudience
		}

		// RFC 8693 token exchange.
		return spiffeauth.NewExchangeTokenSource(minter, spiffeauth.ExchangeConfig{
			TokenURL: openAITokenURL,
			Audience: audience,
			JWTField: "subject_token",
			Body: map[string]string{ //nolint:gosec
				"grant_type":           "urn:ietf:params:oauth:grant-type:token-exchange",
				"subject_token_type":   "urn:ietf:params:oauth:token-type:jwt",
				"identity_provider_id": md.IdentityProviderID,
				"service_account_id":   md.ServiceAccountID,
			},
			Client: conversation.BuildHTTPClient(),
		}), nil

	case strings.EqualFold(md.APIType, "azure") && md.Key == "":
		settings, err := azauth.NewEnvironmentSettings(props)
		if err != nil {
			return nil, fmt.Errorf("error creating Azure environment settings: %w", err)
		}
		cred, err := settings.GetTokenCredential()
		if err != nil {
			return nil, fmt.Errorf("error getting Microsoft Entra ID token credential: %w", err)
		}

		minter, _ := spiffecontext.JWTMinterFrom(ctx)
		return oauth2.ReuseTokenSource(nil, &azureTokenSource{cred: cred, minter: minter}), nil

	default:
		return nil, nil
	}
}

// azureTokenSource gets Microsoft Entra ID tokens for Azure OpenAI.
type azureTokenSource struct {
	cred azcore.TokenCredential
	// minter is the Dapr SPIFFE identity captured at Init, which the SPIFFE
	// credential in the chain reads from the context it is given.
	minter kitspiffe.JWTSVIDMinter
}

func (a *azureTokenSource) Token() (*oauth2.Token, error) {
	ctx, cancel := context.WithTimeout(context.Background(), spiffeauth.ExchangeTimeout)
	defer cancel()
	if a.minter != nil {
		ctx = spiffecontext.WithJWTMinter(ctx, a.minter)
	}

	tok, err := a.cred.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{cognitiveServicesScope}})
	if err != nil {
		return nil, err
	}

	return &oauth2.Token{AccessToken: tok.Token, TokenType: "Bearer", Expiry: tok.ExpiresOn}, nil
}

func (o *OpenAI) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := OpenAILangchainMetadata{}
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (o *OpenAI) Close() error {
	return nil
}
