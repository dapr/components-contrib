/*
Copyright 2023 The Dapr Authors
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
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/ai/azopenai"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"

	"github.com/dapr/components-contrib/bindings"
	azauth "github.com/dapr/components-contrib/common/authentication/azure"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

// List of operations.
const (
	CompletionOperation     bindings.OperationKind = "completion"
	ChatCompletionOperation bindings.OperationKind = "chat-completion"
	GetEmbeddingOperation   bindings.OperationKind = "get-embedding"

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
	// Endpoint is the endpoint for the Azure OpenAI API.
	Endpoint string `mapstructure:"endpoint"`
}

// ChatMessages type for chat completion API.
type ChatMessages struct {
	DeploymentID     string    `json:"deploymentID"`
	Messages         []Message `json:"messages"`
	Temperature      float32   `json:"temperature"`
	MaxTokens        int32     `json:"maxTokens"`
	TopP             float32   `json:"topP"`
	N                int32     `json:"n"`
	PresencePenalty  float32   `json:"presencePenalty"`
	FrequencyPenalty float32   `json:"frequencyPenalty"`
	Stop             []string  `json:"stop"`
}

// Message type stores the messages for bot conversation.
type Message struct {
	Role    string
	Message string
}

// Prompt type for completion API.
type Prompt struct {
	DeploymentID     string   `json:"deploymentID"`
	Prompt           string   `json:"prompt"`
	Temperature      float32  `json:"temperature"`
	MaxTokens        int32    `json:"maxTokens"`
	TopP             float32  `json:"topP"`
	N                int32    `json:"n"`
	PresencePenalty  float32  `json:"presencePenalty"`
	FrequencyPenalty float32  `json:"frequencyPenalty"`
	Stop             []string `json:"stop"`
}

type EmbeddingMessage struct {
	DeploymentID string `json:"deploymentID"`
	Message      string `json:"message"`
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
	err := kitmd.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return fmt.Errorf("error decoding metadata: %w", err)
	}
	if m.Endpoint == "" {
		return fmt.Errorf("required metadata not set: %s", Endpoint)
	}

	if m.APIKey != "" {
		// use API key authentication
		var keyCredential *azcore.KeyCredential
		keyCredential = azcore.NewKeyCredential(m.APIKey)
		if keyCredential == nil {
			return errors.New("error getting credentials object")
		}

		p.client, err = azopenai.NewClientWithKeyCredential(m.Endpoint, keyCredential, nil)
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

		p.client, err = azopenai.NewClient(m.Endpoint, token, nil)
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
		GetEmbeddingOperation,
	}
}

// Invoke handles all invoke operations.
func (p *AzOpenAI) Invoke(ctx context.Context, req *bindings.InvokeRequest) (resp *bindings.InvokeResponse, err error) {
	if req == nil || len(req.Metadata) == 0 {
		return nil, errors.New("invalid request: metadata is required")
	}

	startTime := time.Now().UTC()
	resp = &bindings.InvokeResponse{
		Metadata: map[string]string{
			"operation":  string(req.Operation),
			"start-time": startTime.Format(time.RFC3339Nano),
		},
	}

	switch req.Operation { //nolint:exhaustive
	case CompletionOperation:
		response, err := p.completion(ctx, req.Data, req.Metadata)
		if err != nil {
			return nil, fmt.Errorf("error performing completion: %w", err)
		}
		responseAsBytes, _ := json.Marshal(response)
		resp.Data = responseAsBytes

	case ChatCompletionOperation:
		response, err := p.chatCompletion(ctx, req.Data, req.Metadata)
		if err != nil {
			return nil, fmt.Errorf("error performing chat completion: %w", err)
		}
		responseAsBytes, _ := json.Marshal(response)
		resp.Data = responseAsBytes

	case GetEmbeddingOperation:
		response, err := p.getEmbedding(ctx, req.Data, req.Metadata)
		if err != nil {
			return nil, fmt.Errorf("error performing get embedding operation: %w", err)
		}
		responseAsBytes, _ := json.Marshal(response)
		resp.Data = responseAsBytes

	default:
		return nil, fmt.Errorf(
			"invalid operation type: %s. Expected %s, %s",
			req.Operation, CompletionOperation, ChatCompletionOperation,
		)
	}

	endTime := time.Now().UTC()
	resp.Metadata["end-time"] = endTime.Format(time.RFC3339Nano)
	resp.Metadata["duration"] = endTime.Sub(startTime).String()

	return resp, nil
}

func (p *AzOpenAI) completion(ctx context.Context, message []byte, metadata map[string]string) (response []azopenai.Choice, err error) {
	prompt := Prompt{
		Temperature:      1.0,
		TopP:             1.0,
		MaxTokens:        16,
		N:                1,
		PresencePenalty:  0.0,
		FrequencyPenalty: 0.0,
	}
	err = json.Unmarshal(message, &prompt)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling the input object: %w", err)
	}

	if prompt.Prompt == "" {
		return nil, errors.New("prompt is required for completion operation")
	}

	if prompt.DeploymentID == "" {
		return nil, fmt.Errorf("required metadata not set: %s", DeploymentID)
	}

	if len(prompt.Stop) == 0 {
		prompt.Stop = nil
	}

	resp, err := p.client.GetCompletions(ctx, azopenai.CompletionsOptions{
		DeploymentName: &prompt.DeploymentID,
		Prompt:         []string{prompt.Prompt},
		MaxTokens:      &prompt.MaxTokens,
		Temperature:    &prompt.Temperature,
		TopP:           &prompt.TopP,
		N:              &prompt.N,
		Stop:           prompt.Stop,
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("error getting completion api: %w", err)
	}

	// No choices returned
	if len(resp.Completions.Choices) == 0 {
		return []azopenai.Choice{}, nil
	}

	choices := resp.Completions.Choices
	response = make([]azopenai.Choice, len(choices))
	for i, c := range choices {
		response[i] = c
	}

	return response, nil
}

func (p *AzOpenAI) chatCompletion(ctx context.Context, messageRequest []byte, metadata map[string]string) (response []azopenai.ChatChoice, err error) {
	messages := ChatMessages{
		Temperature:      1.0,
		TopP:             1.0,
		N:                1,
		PresencePenalty:  0.0,
		FrequencyPenalty: 0.0,
	}
	err = json.Unmarshal(messageRequest, &messages)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling the input object: %w", err)
	}

	if len(messages.Messages) == 0 {
		return nil, errors.New("messages are required for chat-completion operation")
	}

	if messages.DeploymentID == "" {
		return nil, fmt.Errorf("required metadata not set: %s", DeploymentID)
	}

	if len(messages.Stop) == 0 {
		messages.Stop = nil
	}

	messageReq := make([]azopenai.ChatRequestMessageClassification, len(messages.Messages))
	for i, m := range messages.Messages {
		currentMsg := m.Message
		switch azopenai.ChatRole(m.Role) {
		case azopenai.ChatRoleUser:
			messageReq[i] = &azopenai.ChatRequestUserMessage{
				Content: azopenai.NewChatRequestUserMessageContent(currentMsg),
			}
		case azopenai.ChatRoleAssistant:
			messageReq[i] = &azopenai.ChatRequestAssistantMessage{
				Content: &currentMsg,
			}
		case azopenai.ChatRoleFunction:
			messageReq[i] = &azopenai.ChatRequestFunctionMessage{
				Content: &currentMsg,
			}
		case azopenai.ChatRoleSystem:
			messageReq[i] = &azopenai.ChatRequestSystemMessage{
				Content: &currentMsg,
			}
		case azopenai.ChatRoleTool:
			messageReq[i] = &azopenai.ChatRequestToolMessage{
				Content: &currentMsg,
			}
		default:
			return nil, fmt.Errorf("invalid role: %s", m.Role)
		}
	}

	var maxTokens *int32
	if messages.MaxTokens != 0 {
		maxTokens = &messages.MaxTokens
	}

	res, err := p.client.GetChatCompletions(ctx, azopenai.ChatCompletionsOptions{
		DeploymentName: &messages.DeploymentID,
		MaxTokens:      maxTokens,
		Temperature:    &messages.Temperature,
		TopP:           &messages.TopP,
		N:              &messages.N,
		Messages:       messageReq,
		Stop:           messages.Stop,
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("error getting chat completion api: %w", err)
	}

	// No choices returned.
	if len(res.ChatCompletions.Choices) == 0 {
		return []azopenai.ChatChoice{}, nil
	}

	choices := res.ChatCompletions.Choices
	response = make([]azopenai.ChatChoice, len(choices))
	for i, c := range choices {
		response[i] = c
	}

	return response, nil
}

func (p *AzOpenAI) getEmbedding(ctx context.Context, messageRequest []byte, metadata map[string]string) (response []float32, err error) {
	message := EmbeddingMessage{}
	err = json.Unmarshal(messageRequest, &message)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling the input object: %w", err)
	}

	if message.DeploymentID == "" {
		return nil, fmt.Errorf("required metadata not set: %s", DeploymentID)
	}

	res, err := p.client.GetEmbeddings(ctx, azopenai.EmbeddingsOptions{
		DeploymentName: &message.DeploymentID,
		Input:          []string{message.Message},
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("error getting embedding api: %w", err)
	}

	// No embedding returned.
	if len(res.Data) == 0 {
		return []float32{}, nil
	}

	response = res.Data[0].Embedding
	return response, nil
}

// Close Az OpenAI instance.
func (p *AzOpenAI) Close() error {
	p.client = nil

	return nil
}

// GetComponentMetadata returns the metadata of the component.
func (p *AzOpenAI) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := openAIMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}
