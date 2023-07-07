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

package openai

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/cognitiveservices/azopenai"

	"github.com/dapr/components-contrib/bindings"
	azauth "github.com/dapr/components-contrib/internal/authentication/azure"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/config"
	"github.com/dapr/kit/logger"
)

// List of operations.
const (
	CompletionOperation     bindings.OperationKind = "completion"
	ChatCompletionOperation bindings.OperationKind = "chatCompletion"

	APIKey           = "apiKey"
	DeploymentID     = "deploymentID"
	Endpoint         = "endpoint"
	MessagesKey      = "messages"
	Temperature      = "temperature"
	MaxTokens        = "maxTokens"
	TopP             = "topP"
	N                = "n"
	Stop             = "stop"
	FrequencyPenalty = "frequencyPenalty"
	LogitBias        = "logitBias"
	User             = "user"
)

// AzOpenAI represents OpenAI output binding.
type AzOpenAI struct {
	logger logger.Logger
	client *azopenai.Client
}

type openAIMetadata struct {
	// APIKey is the API key for the Azure OpenAI API.
	APIKey string `mapstructure:"apiKey"`
	// DeploymentID is the deployment ID for the Azure OpenAI API.
	DeploymentID string `mapstructure:"deploymentID"`
	// Endpoint is the endpoint for the Azure OpenAI API.
	Endpoint string `mapstructure:"endpoint"`
}

type ChatSettings struct {
	Temperature      float32 `mapstructure:"temperature"`
	MaxTokens        int32   `mapstructure:"maxTokens"`
	TopP             float32 `mapstructure:"topP"`
	N                int32   `mapstructure:"n"`
	PresencePenalty  float32 `mapstructure:"presencePenalty"`
	FrequencyPenalty float32 `mapstructure:"frequencyPenalty"`
}

// MessageArray type for chat completion API.
type MessageArray struct {
	Messages []Message
}

type Message struct {
	Role    string
	Message string
}

// Prompt type for completion API.
type Prompt struct {
	Prompt string
}

// NewOpenAI returns a new OpenAI output binding.
func NewOpenAI(logger logger.Logger) bindings.OutputBinding {
	return &AzOpenAI{
		logger: logger,
	}
}

// Init initializes the OpenAI binding.
func (p *AzOpenAI) Init(ctx context.Context, meta bindings.Metadata) error {
	m := openAIMetadata{}
	err := metadata.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return fmt.Errorf("error decoding metadata: %w", err)
	}
	if m.Endpoint == "" {
		return fmt.Errorf("required metadata not set: %s", Endpoint)
	}
	if m.DeploymentID == "" {
		return fmt.Errorf("required metadata not set: %s", DeploymentID)
	}

	if m.APIKey != "" {
		// use API key authentication
		var keyCredential azopenai.KeyCredential
		keyCredential, err = azopenai.NewKeyCredential(m.APIKey)
		if err != nil {
			return fmt.Errorf("error getting credentials object: %w", err)
		}

		p.client, err = azopenai.NewClientWithKeyCredential(m.Endpoint, keyCredential, m.DeploymentID, nil)
		if err != nil {
			return fmt.Errorf("error creating Azure OpenAI client: %w", err)
		}
	} else {
		// fallback to Azure AD authentication
		settings, innerErr := azauth.NewEnvironmentSettings(meta.Properties)
		if innerErr != nil {
			return fmt.Errorf("error creating environment settings: %w", innerErr)
		}

		token, innerErr := settings.GetTokenCredential()
		if innerErr != nil {
			return fmt.Errorf("error getting token credential: %w", innerErr)
		}

		p.client, err = azopenai.NewClient(m.Endpoint, token, m.DeploymentID, nil)
		if err != nil {
			return fmt.Errorf("error creating Azure OpenAI client: %w", err)
		}
	}

	return nil
}

// Operations returns list of operations supported by OpenAI binding.
func (p *AzOpenAI) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		ChatCompletionOperation,
		CompletionOperation,
	}
}

// Invoke handles all invoke operations.
func (p *AzOpenAI) Invoke(ctx context.Context, req *bindings.InvokeRequest) (resp *bindings.InvokeResponse, err error) {
	if req == nil || len(req.Metadata) == 0 {
		return nil, fmt.Errorf("invalid request: metadata is required")
	}

	startTime := time.Now().UTC()
	resp = &bindings.InvokeResponse{
		Metadata: map[string]string{
			"operation": string(req.Operation),
			"startTime": startTime.Format(time.RFC3339),
		},
	}

	switch req.Operation { //nolint:exhaustive
	case CompletionOperation:
		response, err := p.completion(ctx, req.Data, req.Metadata)
		if err != nil {
			return nil, fmt.Errorf("error performing completion: %w", err)
		}
		resp.Metadata["response"] = response

	case ChatCompletionOperation:
		response, err := p.chatCompletion(ctx, req.Data, req.Metadata)
		if err != nil {
			return nil, fmt.Errorf("error performing chat completion: %w", err)
		}
		resp.Metadata["response"] = response

	default:
		return nil, fmt.Errorf(
			"invalid operation type: %s. Expected %s, %s",
			req.Operation, CompletionOperation, ChatCompletionOperation,
		)
	}

	endTime := time.Now().UTC()
	resp.Metadata["endTime"] = endTime.Format(time.RFC3339)
	resp.Metadata["duration"] = endTime.Sub(startTime).String()

	return resp, nil
}

func (s *ChatSettings) Decode(in interface{}) error {
	if err := config.Decode(in, s); err != nil {
		return fmt.Errorf("decode failed. %w", err)
	}

	return nil
}

func (p *AzOpenAI) completion(ctx context.Context, message []byte, metadata map[string]string) (response string, err error) {
	var ma Prompt
	err = json.Unmarshal(message, &ma)
	if err != nil {
		return "", fmt.Errorf("error unmarshalling the message array: %w", err)
	}

	if ma.Prompt == "" {
		return "", fmt.Errorf("prompt is required for completion operation")
	}

	// Refer to the following link for more information on the parameters and their default values:
	// https://platform.openai.com/docs/api-reference/completions/create
	settings := ChatSettings{
		Temperature:      1.0,
		TopP:             1.0,
		MaxTokens:        16,
		N:                1,
		PresencePenalty:  0.0,
		FrequencyPenalty: 0.0,
	}

	err = settings.Decode(metadata)
	if err != nil {
		return "", fmt.Errorf("error decoding the parameters: %w", err)
	}

	resp, err := p.client.GetCompletions(ctx, azopenai.CompletionsOptions{
		Prompt:      []*string{to.Ptr(ma.Prompt)},
		MaxTokens:   &settings.MaxTokens,
		Temperature: &settings.Temperature,
		TopP:        &settings.Temperature,
		N:           &settings.N,
	}, nil)
	if err != nil {
		return "", fmt.Errorf("error getting completion api: %w", err)
	}

	entry := resp.Completions
	if entry.Choices == nil || len(entry.Choices) == 0 {
		return "", fmt.Errorf("error getting response from completion api: %w", err)
	}
	response = *entry.Choices[0].Text

	return response, nil
}

func (p *AzOpenAI) chatCompletion(ctx context.Context, messageArray []byte, metadata map[string]string) (response string, err error) {
	var ma MessageArray
	err = json.Unmarshal(messageArray, &ma)
	if err != nil {
		return "", fmt.Errorf("error unmarshalling the message array: %w", err)
	}

	if len(ma.Messages) == 0 {
		return "", fmt.Errorf("messages are required for chatCompletion operation")
	}

	// Refer to the following link for more information on the parameters and their default values:
	// https://platform.openai.com/docs/api-reference/chat/create
	settings := ChatSettings{
		Temperature:      1.0,
		TopP:             1.0,
		N:                1,
		PresencePenalty:  0.0,
		FrequencyPenalty: 0.0,
	}

	err = settings.Decode(metadata)
	if err != nil {
		return "", fmt.Errorf("error decoding the parameters: %w", err)
	}

	messageReq := []*azopenai.ChatMessage{}
	for _, message := range ma.Messages {
		messageReq = append(messageReq, &azopenai.ChatMessage{
			Role:    to.Ptr(azopenai.ChatRole(message.Role)),
			Content: to.Ptr(message.Message),
		})
	}

	var maxTokens *int32
	if settings.MaxTokens != 0 {
		maxTokens = &settings.MaxTokens
	}

	res, err := p.client.GetChatCompletions(ctx, azopenai.ChatCompletionsOptions{
		MaxTokens:   maxTokens,
		Temperature: &settings.Temperature,
		TopP:        &settings.TopP,
		N:           &settings.N,
		Messages:    messageReq,
	}, nil)
	if err != nil {
		return "", fmt.Errorf("error getting chat completion api: %w", err)
	}

	entry := res.ChatCompletions
	if entry.Choices == nil || len(entry.Choices) == 0 {
		return "", fmt.Errorf("error getting response from chat completion api: %w", err)
	}

	response = *entry.Choices[0].Message.Content

	return response, nil
}

// Close Az OpenAI instance.
func (p *AzOpenAI) Close() error {
	p.client = nil

	return nil
}

// GetComponentMetadata returns the metadata of the component.
func (p *AzOpenAI) GetComponentMetadata() map[string]string {
	metadataStruct := openAIMetadata{}
	metadataInfo := map[string]string{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return metadataInfo
}
