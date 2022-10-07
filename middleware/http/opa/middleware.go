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

package opa

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"
	"time"

	"github.com/open-policy-agent/opa/rego"
	"k8s.io/utils/strings/slices"

	"github.com/dapr/components-contrib/internal/httputils"
	"github.com/dapr/components-contrib/internal/utils"
	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

type Status int

type middlewareMetadata struct {
	Rego                  string   `json:"rego"`
	DefaultStatus         Status   `json:"defaultStatus,omitempty"`
	IncludedHeaders       string   `json:"includedHeaders,omitempty"`
	ReadBody              string   `json:"readBody,omitempty"`
	includedHeadersParsed []string `json:"-"`
}

// NewMiddleware returns a new Open Policy Agent middleware.
func NewMiddleware(logger logger.Logger) middleware.Middleware {
	return &Middleware{logger: logger}
}

// Middleware is an OPA  middleware.
type Middleware struct {
	logger logger.Logger
}

// RegoResult is the expected result from rego policy.
type RegoResult struct {
	Allow             bool              `json:"allow"`
	AdditionalHeaders map[string]string `json:"additional_headers,omitempty"`
	StatusCode        int               `json:"status_code,omitempty"`
}

const opaErrorHeaderKey = "x-dapr-opa-error"

var (
	errOpaNoResult          = errors.New("received no results back from rego policy. Are you setting data.http.allow?")
	errOpaInvalidResultType = errors.New("got an invalid type back from repo policy. Only a boolean or map is valid")
)

func (s *Status) UnmarshalJSON(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		if value != math.Trunc(value) {
			return fmt.Errorf("invalid float value %f parse to status(int)", value)
		}
		*s = Status(value)
	case string:
		intVal, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		*s = Status(intVal)
	default:
		return fmt.Errorf("invalid value %v parse to status(int)", value)
	}
	if !s.Valid() {
		return fmt.Errorf("invalid status value %d expected in range [100-599]", *s)
	}

	return nil
}

// Check status is in the correct range for RFC 2616 status codes [100-599].
func (s *Status) Valid() bool {
	return s != nil && *s >= 100 && *s < 600
}

// GetHandler returns the HTTP handler provided by the middleware.
func (m *Middleware) GetHandler(metadata middleware.Metadata) (func(next http.Handler) http.Handler, error) {
	meta, err := m.getNativeMetadata(metadata)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
	query, err := rego.New(
		rego.Query("result = data.http.allow"),
		rego.Module("inline.rego", meta.Rego),
	).PrepareForEval(ctx)
	cancel()
	if err != nil {
		return nil, err
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if allow := m.evalRequest(w, r, meta, &query); !allow {
				return
			}
			next.ServeHTTP(w, r)
		})
	}, nil
}

func (m *Middleware) evalRequest(w http.ResponseWriter, r *http.Request, meta *middlewareMetadata, query *rego.PreparedEvalQuery) bool {
	headers := map[string]string{}

	for key, value := range r.Header {
		if len(value) > 0 && slices.Contains(meta.includedHeadersParsed, key) {
			headers[key] = strings.Join(value, ", ")
		}
	}

	var body string
	if utils.IsTruthy(meta.ReadBody) {
		buf, _ := io.ReadAll(r.Body)
		body = string(buf)

		// Put the body back in the request
		r.Body = io.NopCloser(bytes.NewBuffer(buf))
	}

	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	input := map[string]interface{}{
		"request": map[string]interface{}{
			"method":     r.Method,
			"path":       r.URL.Path,
			"path_parts": pathParts,
			"raw_query":  r.URL.RawQuery,
			"query":      map[string][]string(r.URL.Query()),
			"headers":    headers,
			"scheme":     r.URL.Scheme,
			"body":       body,
		},
	}

	results, err := query.Eval(r.Context(), rego.EvalInput(input))
	if err != nil {
		m.opaError(w, meta, err)
		return false
	}

	if len(results) == 0 {
		m.opaError(w, meta, errOpaNoResult)
		return false
	}

	return m.handleRegoResult(w, meta, results[0].Bindings["result"])
}

// handleRegoResult takes the in process request and open policy agent evaluation result
// and maps it the appropriate response or headers.
// It returns true if the request should continue, or false if a response should be immediately returned.
func (m *Middleware) handleRegoResult(w http.ResponseWriter, meta *middlewareMetadata, result any) bool {
	if allowed, ok := result.(bool); ok {
		if !allowed {
			httputils.RespondWithError(w, int(meta.DefaultStatus))
		}
		return allowed
	}

	if _, ok := result.(map[string]any); !ok {
		m.opaError(w, meta, errOpaInvalidResultType)
		return false
	}

	// Is it expensive to marshal back and forth? Should we just manually pull out properties?
	marshaled, err := json.Marshal(result)
	if err != nil {
		m.opaError(w, meta, err)
		return false
	}

	regoResult := RegoResult{
		// By default, a non-allowed request with return a 403 response.
		StatusCode:        int(meta.DefaultStatus),
		AdditionalHeaders: make(map[string]string),
	}

	if err = json.Unmarshal(marshaled, &regoResult); err != nil {
		m.opaError(w, meta, err)
		return false
	}

	// Set the headers on the ongoing request (overriding as necessary)
	for key, value := range regoResult.AdditionalHeaders {
		w.Header().Set(key, value)
	}

	// If the result isn't allowed, set the response status
	if !regoResult.Allow {
		httputils.RespondWithError(w, regoResult.StatusCode)
	}

	return regoResult.Allow
}

func (m *Middleware) opaError(w http.ResponseWriter, meta *middlewareMetadata, err error) {
	w.Header().Set(opaErrorHeaderKey, "true")
	httputils.RespondWithError(w, int(meta.DefaultStatus))
	m.logger.Warnf("Error procesing rego policy: %v", err)
}

func (m *Middleware) getNativeMetadata(metadata middleware.Metadata) (*middlewareMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	meta := middlewareMetadata{
		DefaultStatus: 403,
	}
	err = json.Unmarshal(b, &meta)
	if err != nil {
		return nil, err
	}

	meta.includedHeadersParsed = strings.Split(meta.IncludedHeaders, ",")
	n := 0
	for i := range meta.includedHeadersParsed {
		scrubbed := strings.ReplaceAll(meta.includedHeadersParsed[i], " ", "")
		if scrubbed != "" {
			meta.includedHeadersParsed[n] = textproto.CanonicalMIMEHeaderKey(scrubbed)
			n++
		}
	}
	meta.includedHeadersParsed = meta.includedHeadersParsed[:n]

	return &meta, nil
}
