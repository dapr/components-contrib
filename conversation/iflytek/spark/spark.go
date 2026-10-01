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

package spark

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/langchaingokit"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"

	"github.com/tmc/langchaingo/llms/openai"
)

type Spark struct {
	langchaingokit.LLM
	md SparkMetadata

	logger logger.Logger
}

// Defaults target the iFlytek MaaS OpenAI-compatible API, which issues the keys new users get.
// Keys for the legacy Spark HTTP API need endpoint https://spark-api-open.xf-yun.com/v1 and a model such as 4.0Ultra.
const (
	defaultModel    = "spark-x2.5"
	defaultEndpoint = "https://maas-api.cn-huabei-1.xf-yun.com/v2"
	// With thinking enabled (the API default), Spark X models regularly return only reasoning
	// content, which surfaces as an empty response with no tool calls.
	defaultThinking = "disabled"
)

func NewSpark(logger logger.Logger) conversation.Conversation {
	o := &Spark{
		logger: logger,
		LLM:    langchaingokit.New(logger),
	}

	return o
}

func (s *Spark) Init(ctx context.Context, meta conversation.Metadata) error {
	md := SparkMetadata{}
	err := kmeta.DecodeMetadata(meta.Properties, &md)
	if err != nil {
		return err
	}
	if md.Key == "" {
		return errors.New("spark api key is required")
	}

	model := defaultModel
	if md.Model != "" {
		model = md.Model
	}

	if md.Endpoint == "" {
		md.Endpoint = defaultEndpoint
	}

	thinking, err := resolveThinking(md.Thinking, md.Endpoint)
	if err != nil {
		return err
	}
	md.Thinking = thinking

	options := conversation.BuildOpenAIClientOptions(model, md.Key, md.Endpoint)
	if thinking != "" {
		options = append(options, openai.WithHTTPClient(&thinkingClient{
			doer:     conversation.BuildHTTPClient(),
			thinking: thinking,
		}))
	}
	llm, err := openai.New(options...)
	if err != nil {
		return err
	}

	s.Model = llm
	s.SetModel(model)
	s.md = md

	if md.ResponseCacheTTL != nil {
		cachedModel, cacheErr := conversation.CacheResponses(ctx, md.ResponseCacheTTL, s.Model)
		if cacheErr != nil {
			return cacheErr
		}

		s.Model = cachedModel
	}
	return nil
}

// resolveThinking returns the thinking mode to send, or "" to leave requests unchanged.
// The default applies to the default endpoint only, since other Spark APIs may not accept the field.
func resolveThinking(configured, endpoint string) (string, error) {
	if configured == "" {
		if endpoint == defaultEndpoint {
			return defaultThinking, nil
		}
		return "", nil
	}
	thinking := strings.ToLower(strings.TrimSpace(configured))
	switch thinking {
	case "enabled", "disabled", "auto":
		return thinking, nil
	default:
		return "", fmt.Errorf("invalid thinking mode %q: must be enabled, disabled or auto", configured)
	}
}

type doer interface {
	Do(req *http.Request) (*http.Response, error)
}

// thinkingClient adds the thinking mode to chat completion requests, as the OpenAI client
// used by langchaingo has no way to send extra request fields.
type thinkingClient struct {
	doer     doer
	thinking string
}

func (c *thinkingClient) Do(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodPost || req.Body == nil || !strings.HasSuffix(req.URL.Path, "/chat/completions") {
		return c.doer.Do(req)
	}

	body, err := io.ReadAll(req.Body)
	_ = req.Body.Close()
	if err != nil {
		return nil, err
	}

	var payload map[string]json.RawMessage
	if json.Unmarshal(body, &payload) == nil {
		if _, ok := payload["thinking"]; !ok {
			payload["thinking"], _ = json.Marshal(map[string]string{"type": c.thinking})
			if patched, marshalErr := json.Marshal(payload); marshalErr == nil {
				body = patched
			}
		}
	}

	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
	return c.doer.Do(req)
}

func (s *Spark) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := SparkMetadata{}
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (s *Spark) Close() error {
	return nil
}
