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
package anthropic

import (
	"context"
	"errors"
	"reflect"
	"strings"

	spiffeauth "github.com/dapr/components-contrib/common/authentication/spiffe"
	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/langchaingokit"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"

	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/anthropic"
	"golang.org/x/oauth2"
)

const (
	apiTypeFoundry = "foundry"

	defaultBaseURL            = "https://api.anthropic.com/v1"
	defaultFederationAudience = "https://api.anthropic.com"
)

type Anthropic struct {
	langchaingokit.LLM

	logger logger.Logger
}

func NewAnthropic(logger logger.Logger) conversation.Conversation {
	a := &Anthropic{
		logger: logger,
		LLM:    langchaingokit.New(logger),
	}
	a.SetProvider(langchaingokit.ProviderAnthropic)

	return a
}

func (a *Anthropic) buildClientOptions(ctx context.Context, md AnthropicLangchainMetadata) (string, []anthropic.Option, error) {
	model := conversation.GetAnthropicModel(md.Model)

	options := []anthropic.Option{
		anthropic.WithModel(model),
	}

	isFoundry := strings.EqualFold(md.APIType, apiTypeFoundry)
	if isFoundry && md.Endpoint == "" {
		return "", nil, errors.New("endpoint must be provided when apiType is set to 'foundry'")
	}

	baseURL := defaultBaseURL
	if md.Endpoint != "" {
		baseURL = strings.TrimSuffix(md.Endpoint, "/")
		options = append(options, anthropic.WithBaseURL(baseURL))
	}

	if md.FederationRuleID == "" {
		options = append(options,
			anthropic.WithToken(md.Key),
			anthropic.WithHTTPClient(conversation.BuildHTTPClient()),
		)
		return model, options, nil
	}

	if isFoundry {
		return "", nil, errors.New("workload identity federation is not supported when apiType is set to 'foundry'")
	}
	ts, err := federationTokenSource(ctx, md, baseURL)
	if err != nil {
		return "", nil, err
	}
	options = append(options,
		anthropic.WithToken(conversation.PlaceholderToken),
		anthropic.WithHTTPClient(conversation.BuildBearerHTTPClient(ts)),
	)

	return model, options, nil
}

// federationTokenSource exchanges JWT SVIDs for Anthropic access tokens with
// the RFC 7523 JWT bearer grant.
func federationTokenSource(ctx context.Context, md AnthropicLangchainMetadata, baseURL string) (oauth2.TokenSource, error) {
	if md.OrganizationID == "" || md.ServiceAccountID == "" {
		return nil, errors.New("organizationId and serviceAccountId must be provided when federationRuleId is set")
	}

	minter, err := spiffeauth.MinterFrom(ctx)
	if err != nil {
		return nil, err
	}

	audience := md.Audience
	if audience == "" {
		audience = defaultFederationAudience
	}

	body := map[string]string{
		"grant_type":         "urn:ietf:params:oauth:grant-type:jwt-bearer",
		"federation_rule_id": md.FederationRuleID,
		"organization_id":    md.OrganizationID,
		"service_account_id": md.ServiceAccountID,
	}
	if md.WorkspaceID != "" {
		body["workspace_id"] = md.WorkspaceID
	}

	return spiffeauth.NewExchangeTokenSource(minter, spiffeauth.ExchangeConfig{
		TokenURL: baseURL + "/oauth/token",
		Audience: audience,
		JWTField: "assertion",
		Body:     body,
		Client:   conversation.BuildHTTPClient(),
	}), nil
}

func (a *Anthropic) Init(ctx context.Context, meta conversation.Metadata) error {
	md := AnthropicLangchainMetadata{}
	err := kmeta.DecodeMetadata(meta.Properties, &md)
	if err != nil {
		return err
	}

	model, options, err := a.buildClientOptions(ctx, md)
	if err != nil {
		return err
	}

	llm, err := anthropic.New(options...)
	if err != nil {
		return err
	}

	a.Model = splitParts{llm}
	a.SetModel(model)
	a.SetDefaultMaxTokens(md.MaxTokens)

	if md.ResponseCacheTTL != nil {
		cachedModel, cacheErr := conversation.CacheResponses(ctx, md.ResponseCacheTTL, a.Model)
		if cacheErr != nil {
			return cacheErr
		}

		a.Model = cachedModel
	}

	return nil
}

// splitParts works around the pinned langchaingo Anthropic client, which
// converts only the first part of an AI message. daprd sends an assistant turn
// as its text, then one ToolCall part per call, so every tool_use after the
// first part was dropped and Anthropic rejected the tool_result blocks that
// followed. Sending each part as its own AI message keeps them all, and
// Anthropic combines consecutive assistant turns into one.
// TODO: remove once a langchaingo release has both structured output (which
// the pin is on a branch for) and upstream commit f294eeec6.
type splitParts struct {
	*anthropic.LLM
}

func (s splitParts) GenerateContent(ctx context.Context, messages []llms.MessageContent, options ...llms.CallOption) (*llms.ContentResponse, error) {
	split := make([]llms.MessageContent, 0, len(messages))
	for _, msg := range messages {
		if msg.Role != llms.ChatMessageTypeAI || len(msg.Parts) < 2 {
			split = append(split, msg)
			continue
		}
		for _, part := range msg.Parts {
			split = append(split, llms.MessageContent{Role: msg.Role, Parts: []llms.ContentPart{part}})
		}
	}
	return s.LLM.GenerateContent(ctx, split, options...)
}

func (a *Anthropic) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := AnthropicLangchainMetadata{}
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (a *Anthropic) Close() error {
	return nil
}
