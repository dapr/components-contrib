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

package openrouter

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"
)

func decodeRoutingFields(t *testing.T, props map[string]string) (map[string]json.RawMessage, error) {
	t.Helper()
	md := OpenrouterMetadata{}
	require.NoError(t, kmeta.DecodeMetadata(props, &md))
	return buildRoutingFields(md)
}

func TestBuildRoutingFields(t *testing.T) {
	t.Run("nothing configured", func(t *testing.T) {
		fields, err := decodeRoutingFields(t, map[string]string{"key": "test-key"})
		require.NoError(t, err)
		assert.Empty(t, fields)
	})

	t.Run("all fields", func(t *testing.T) {
		fields, err := decodeRoutingFields(t, map[string]string{
			"fallbackModels":         "openai/gpt-5-mini, anthropic/claude-haiku-4.5,",
			"providerOrder":          "anthropic,google-vertex",
			"providerOnly":           "anthropic, google-vertex, azure",
			"providerIgnore":         "deepinfra",
			"providerSort":           "Price",
			"allowProviderFallbacks": "false",
			"requireParameters":      "true",
			"dataCollection":         "deny",
			"zdr":                    "true",
			"quantizations":          "fp8, FP16",
			"maxPromptPrice":         "1",
			"maxCompletionPrice":     "4.5",
		})
		require.NoError(t, err)

		assert.JSONEq(t, `["openai/gpt-5-mini","anthropic/claude-haiku-4.5"]`, string(fields["models"]))
		assert.JSONEq(t, `{
			"order": ["anthropic","google-vertex"],
			"only": ["anthropic","google-vertex","azure"],
			"ignore": ["deepinfra"],
			"sort": "price",
			"allow_fallbacks": false,
			"require_parameters": true,
			"data_collection": "deny",
			"zdr": true,
			"quantizations": ["fp8","fp16"],
			"max_price": {"prompt": 1, "completion": 4.5}
		}`, string(fields["provider"]))
	})

	t.Run("a zero price is sent", func(t *testing.T) {
		fields, err := decodeRoutingFields(t, map[string]string{"maxCompletionPrice": "0"})
		require.NoError(t, err)
		assert.JSONEq(t, `{"max_price":{"completion":0}}`, string(fields["provider"]))
		assert.NotContains(t, fields, "models")
	})

	t.Run("only fallback models", func(t *testing.T) {
		fields, err := decodeRoutingFields(t, map[string]string{"fallbackModels": "openai/gpt-5-mini"})
		require.NoError(t, err)
		assert.JSONEq(t, `["openai/gpt-5-mini"]`, string(fields["models"]))
		assert.NotContains(t, fields, "provider")
	})

	for name, test := range map[string]struct {
		props   map[string]string
		wantErr string
	}{
		"invalid providerSort":        {props: map[string]string{"providerSort": "cheapest"}, wantErr: "invalid value \"cheapest\" for providerSort"},
		"invalid dataCollection":      {props: map[string]string{"dataCollection": "never"}, wantErr: "invalid value \"never\" for dataCollection"},
		"invalid quantization":        {props: map[string]string{"quantizations": "fp8,q4"}, wantErr: "invalid value \"q4\" for quantizations"},
		"negative maxPromptPrice":     {props: map[string]string{"maxPromptPrice": "-1"}, wantErr: "maxPromptPrice must not be negative"},
		"negative maxCompletionPrice": {props: map[string]string{"maxCompletionPrice": "-0.5"}, wantErr: "maxCompletionPrice must not be negative"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := decodeRoutingFields(t, test.props)
			require.ErrorContains(t, err, test.wantErr)
		})
	}

	t.Run("Init fails on invalid routing metadata", func(t *testing.T) {
		err := NewOpenrouter(logger.NewLogger("test")).Init(t.Context(), conversation.Metadata{Base: metadata.Base{Properties: map[string]string{
			"key":          "test-key",
			"providerSort": "cheapest",
		}}})
		require.ErrorContains(t, err, "providerSort")
	})

	t.Run("non-numeric price fails to decode", func(t *testing.T) {
		md := OpenrouterMetadata{}
		require.Error(t, kmeta.DecodeMetadata(map[string]string{"maxPromptPrice": "cheap"}, &md))
	})
}

type recordingTransport struct {
	req  *http.Request
	body []byte
}

func (r *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r.req = req
	if req.Body != nil {
		r.body, _ = io.ReadAll(req.Body)
	}
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Request: req}, nil
}

// roundTrip sends req through tr and closes the response body.
func roundTrip(tr http.RoundTripper, req *http.Request) error {
	resp, err := tr.RoundTrip(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	return err
}

func TestBodyFieldsTransport(t *testing.T) {
	fields := map[string]json.RawMessage{
		"models":   json.RawMessage(`["openai/gpt-5-mini"]`),
		"provider": json.RawMessage(`{"zdr":true}`),
	}

	t.Run("adds fields to chat completion requests", func(t *testing.T) {
		base := &recordingTransport{}
		tr := &bodyFieldsTransport{base: base, fields: fields}

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://openrouter.ai/api/v1/chat/completions",
			strings.NewReader(`{"model":"openai/gpt-4o-mini","messages":[]}`))
		require.NoError(t, err)
		err = roundTrip(tr, req)
		require.NoError(t, err)

		assert.JSONEq(t, `{"model":"openai/gpt-4o-mini","messages":[],"models":["openai/gpt-5-mini"],"provider":{"zdr":true}}`, string(base.body))
		assert.Equal(t, int64(len(base.body)), base.req.ContentLength)

		require.NotNil(t, base.req.GetBody)
		replay, err := base.req.GetBody()
		require.NoError(t, err)
		replayed, err := io.ReadAll(replay)
		require.NoError(t, err)
		assert.Equal(t, base.body, replayed)
	})

	t.Run("does not replace fields already in the body", func(t *testing.T) {
		base := &recordingTransport{}
		tr := &bodyFieldsTransport{base: base, fields: fields}

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://openrouter.ai/api/v1/chat/completions",
			strings.NewReader(`{"model":"m","provider":{"sort":"price"}}`))
		require.NoError(t, err)
		err = roundTrip(tr, req)
		require.NoError(t, err)

		assert.JSONEq(t, `{"model":"m","models":["openai/gpt-5-mini"],"provider":{"sort":"price"}}`, string(base.body))
	})

	t.Run("leaves other requests unchanged", func(t *testing.T) {
		for _, test := range []struct {
			method string
			url    string
		}{
			{http.MethodGet, "https://openrouter.ai/api/v1/models"},
			{http.MethodPost, "https://openrouter.ai/api/v1/embeddings"},
		} {
			base := &recordingTransport{}
			tr := &bodyFieldsTransport{base: base, fields: fields}

			var body io.Reader
			if test.method == http.MethodPost {
				body = bytes.NewReader([]byte(`{"input":"x"}`))
			}
			req, err := http.NewRequestWithContext(t.Context(), test.method, test.url, body)
			require.NoError(t, err)
			err = roundTrip(tr, req)
			require.NoError(t, err)

			assert.Same(t, req, base.req, "%s %s", test.method, test.url)
		}
	})

	t.Run("fails on a body that is not a JSON object", func(t *testing.T) {
		base := &recordingTransport{}
		tr := &bodyFieldsTransport{base: base, fields: fields}

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://openrouter.ai/api/v1/chat/completions",
			strings.NewReader(`not json`))
		require.NoError(t, err)
		err = roundTrip(tr, req)
		require.Error(t, err)
		assert.Nil(t, base.req)
	})
}

// TestRoutingOnTheWire checks the request OpenRouter receives when routing
// and attribution are both configured.
func TestRoutingOnTheWire(t *testing.T) {
	var (
		body    map[string]json.RawMessage
		headers http.Header
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if !assert.NoError(t, err) {
			return
		}
		assert.NoError(t, json.Unmarshal(raw, &body))
		assert.Equal(t, int64(len(raw)), r.ContentLength)
		headers = r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, chatCompletionsReply)
	}))
	t.Cleanup(srv.Close)

	o := NewOpenrouter(logger.NewLogger("test"))
	require.NoError(t, o.Init(t.Context(), conversation.Metadata{Base: metadata.Base{Properties: map[string]string{
		"key":               "test-key",
		"endpoint":          srv.URL,
		"model":             "anthropic/claude-sonnet-4.6",
		"fallbackModels":    "openai/gpt-5-mini",
		"dataCollection":    "deny",
		"requireParameters": "true",
		"appURL":            "https://myapp.example.com",
	}}}))

	msgs := []llms.MessageContent{llms.TextParts(llms.ChatMessageTypeHuman, "hi")}
	resp, err := o.Converse(t.Context(), &conversation.Request{Message: &msgs})
	require.NoError(t, err)
	require.Len(t, resp.Outputs, 1)

	require.NotNil(t, body)
	assert.JSONEq(t, `"anthropic/claude-sonnet-4.6"`, string(body["model"]))
	assert.JSONEq(t, `["openai/gpt-5-mini"]`, string(body["models"]))
	assert.JSONEq(t, `{"data_collection":"deny","require_parameters":true}`, string(body["provider"]))
	assert.Contains(t, body, "messages")

	assert.Equal(t, "Bearer test-key", headers.Get("Authorization"))
	assert.Equal(t, "https://myapp.example.com", headers.Get("HTTP-Referer"))
}
