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
package openai

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// reasoningEffortDoer sets reasoning_effort on every chat completions request.
//
// langchaingo maps llms.WithReasoning to "low", "medium" or "high" only, so it
// cannot send "none". OpenAI needs "none" before a reasoning model accepts
// function tools on /v1/chat/completions, so the field is written into the
// request body here. A reasoning_effort already in the body is left alone.
type reasoningEffortDoer struct {
	next   *http.Client
	effort string
}

func (d *reasoningEffortDoer) Do(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodPost || req.Body == nil || !strings.HasSuffix(req.URL.Path, "/chat/completions") {
		return d.next.Do(req)
	}

	raw, err := io.ReadAll(req.Body)
	req.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to read chat completions request: %w", err)
	}

	var body map[string]json.RawMessage
	if err = json.Unmarshal(raw, &body); err != nil {
		return nil, fmt.Errorf("failed to decode chat completions request: %w", err)
	}
	if _, ok := body["reasoning_effort"]; !ok {
		body["reasoning_effort"], err = json.Marshal(d.effort)
		if err != nil {
			return nil, fmt.Errorf("failed to encode reasoning_effort: %w", err)
		}
		if raw, err = json.Marshal(body); err != nil {
			return nil, fmt.Errorf("failed to encode chat completions request: %w", err)
		}
	}

	req.Body = io.NopCloser(bytes.NewReader(raw))
	req.ContentLength = int64(len(raw))
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(raw)), nil
	}
	return d.next.Do(req)
}
