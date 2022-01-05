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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	fh "github.com/valyala/fasthttp"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

// mockedRequestHandler acts like an upstream service returns success status code 200 and a fixed response body.
func mockedRequestHandler(ctx *fh.RequestCtx) {
	ctx.Response.SetStatusCode(200)
	ctx.Response.SetBody([]byte("from mock"))
}

type RequestConfiguator func(*fh.RequestCtx)

func TestOpaPolicy(t *testing.T) {
	tests := map[string]struct {
		meta               middleware.Metadata
		req                RequestConfiguator
		status             int
		headers            *[][]string
		body               []string
		shouldHandlerError bool
		shouldRegoError    bool
	}{
		"allow": {
			meta: middleware.Metadata{
				Properties: map[string]string{
					"rego": `
						package http
						allow = true`,
				},
			},
			status: 200,
		},
		"deny": {
			meta: middleware.Metadata{
				Properties: map[string]string{
					"rego": `
						package http
						allow = false`,
				},
			},
			status: 403,
		},
		"status": {
			meta: middleware.Metadata{
				Properties: map[string]string{
					"rego": `
						package http
						allow = {
							"allow": false,
							"status_code": 301
						}`,
				},
			},
			status: 301,
		},
		"add redirect": {
			meta: middleware.Metadata{
				Properties: map[string]string{
					"rego": `
						package http
						allow = {
							"allow": false,
							"status_code": 301,
							"additional_headers": { "location": "https://my.site/login" }
						}`,
				},
			},
			status: 301,
			headers: &[][]string{
				{"location", "https://my.site/login"},
			},
		},
		"add headers": {
			meta: middleware.Metadata{
				Properties: map[string]string{
					"rego": `
						package http
						allow = {
							"allow": false,
							"status_code": 301,
							"additional_headers": { "x-key": "abc" }
						}`,
				},
			},
			status: 301,
			headers: &[][]string{
				{"x-key", "abc"},
			},
		},
		"allow with path": {
			meta: middleware.Metadata{
				Properties: map[string]string{
					"rego": `
						package http
						default allow = true

						allow = { "status_code": 403 } {
							input.request.path_parts[0] = "forbidden"
						}
						`,
				},
			},
			req: func(ctx *fh.RequestCtx) {
				ctx.Request.SetHost("https://my.site")
				ctx.Request.URI().SetPath("/allowed")
			},
			status: 200,
		},
		"deny with path": {
			meta: middleware.Metadata{
				Properties: map[string]string{
					"rego": `
						package http
						default allow = true

						allow = { "status_code": 403 } {
							input.request.path_parts[0] = "forbidden"
						}
						`,
				},
			},
			req: func(ctx *fh.RequestCtx) {
				ctx.Request.SetHost("https://my.site")
				ctx.Request.URI().SetPath("/forbidden")
			},
			status: 403,
		},
		"allow when header not included": {
			meta: middleware.Metadata{
				Properties: map[string]string{
					"rego": `
						package http
						default allow = true

						allow = { "status_code": 403 } {
							input.request.headers["x-bad-header"] = "1"
						}
						`,
				},
			},
			req: func(ctx *fh.RequestCtx) {
				ctx.Request.SetHost("https://my.site")
				ctx.Request.Header.Add("x-bad-header", "1")
			},
			status: 200,
		},
		"deny when header included": {
			meta: middleware.Metadata{
				Properties: map[string]string{
					"rego": `
						package http
						default allow = true

						allow = { "status_code": 403 } {
							input.request.headers["X-Bad-Header"] = "1"
						}
						`,
					"includedHeaders": "x-bad-header",
				},
			},
			req: func(ctx *fh.RequestCtx) {
				ctx.Request.SetHost("https://my.site")
				ctx.Request.Header.Add("x-bad-header", "1")
			},
			status: 403,
		},
		"err on no rego": {
			meta: middleware.Metadata{
				Properties: map[string]string{},
			},
			shouldHandlerError: true,
		},
		"err on bad allow": {
			meta: middleware.Metadata{
				Properties: map[string]string{
					"rego": `
						package http
						allow = 1`,
				},
			},
			shouldRegoError: true,
		},
		"err on bad package": {
			meta: middleware.Metadata{
				Properties: map[string]string{
					"rego": `
						package http.authz
						allow = true`,
				},
			},
			shouldRegoError: true,
		},
		"status config": {
			meta: middleware.Metadata{
				Properties: map[string]string{
					"rego": `
						package http
						allow = false`,
					"defaultStatus": "500",
				},
			},
			status: 500,
		},
		"rego priority over defaultStatus metadata": {
			meta: middleware.Metadata{
				Properties: map[string]string{
					"rego": `
						package http
						allow = {
							"allow": false,
							"status_code": 301
						}`,
					"defaultStatus": "500",
				},
			},
			status: 301,
		},
		"allow on body contains allow": {
			meta: middleware.Metadata{
				Properties: map[string]string{
					"rego": `
						package http
						default allow = false
						
						allow = { "status_code": 200 } {
							input.request.body == "allow"
						}
						`,
				},
			},
			req: func(ctx *fh.RequestCtx) {
				ctx.SetContentType("text/plain; charset=utf8")
				ctx.Request.SetHost("https://my.site")
				ctx.Request.SetBodyString("allow")
			},
			status: 200,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			log := logger.NewLogger("opa.test")
			opaMiddleware := NewMiddleware(log)
			handler, err := opaMiddleware.GetHandler(test.meta)

			if test.shouldHandlerError {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			var reqCtx fh.RequestCtx
			if test.req != nil {
				test.req(&reqCtx)
			}
			handler(mockedRequestHandler)(&reqCtx)

			if test.shouldRegoError {
				assert.Equal(t, 403, reqCtx.Response.StatusCode())
				assert.Equal(t, "true", string(reqCtx.Response.Header.Peek(opaErrorHeaderKey)))

				return
			}

			assert.Equal(t, test.status, reqCtx.Response.StatusCode())

			if test.headers != nil {
				for _, header := range *test.headers {
					assert.Equal(t, header[1], string(reqCtx.Response.Header.Peek(header[0])))
				}
			}
		})
	}
}

func TestStatus_UnmarshalJSON(t *testing.T) {
	type testObj struct {
		Value Status `json:"value,omitempty"`
	}
	tests := map[string]struct {
		jsonBytes   []byte
		expectValue Status
		expectError bool
	}{
		"int value": {
			jsonBytes:   []byte(`{"value":100}`),
			expectValue: Status(100),
			expectError: false,
		},
		"string value": {
			jsonBytes:   []byte(`{"value":"100"}`),
			expectValue: Status(100),
			expectError: false,
		},
		"empty value": {
			jsonBytes:   []byte(`{}`),
			expectValue: Status(0),
			expectError: false,
		},
		"invalid status code value": {
			jsonBytes:   []byte(`{"value":600}`),
			expectError: true,
		},
		"invalid float value": {
			jsonBytes:   []byte(`{"value":2.9}`),
			expectError: true,
		},
		"invalid value null": {
			jsonBytes:   []byte(`{"value":null}`),
			expectError: true,
		},
		"invalid value []": {
			jsonBytes:   []byte(`{"value":[]}`),
			expectError: true,
		},
		"invalid value {}": {
			jsonBytes:   []byte(`{"value":{}}`),
			expectError: true,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var obj testObj
			err := json.Unmarshal(test.jsonBytes, &obj)
			if test.expectError {
				assert.NotEmpty(t, err)

				return
			}
			assert.Nil(t, err)
			assert.Equal(t, obj.Value, test.expectValue)
		})
	}
}
