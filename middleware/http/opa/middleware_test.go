package opa

import (
	"testing"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	fh "github.com/valyala/fasthttp"
)

func mockedRequestHandler(ctx *fh.RequestCtx) {}

type RequestConfiguator func(*fh.RequestCtx)

func TestOpaPolicy(t *testing.T) {
	tests := map[string]struct {
		meta        middleware.Metadata
		req         RequestConfiguator
		status      int
		headers     *[][]string
		shouldError *bool
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
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			log := logger.NewLogger("opa.test")
			opaMiddleware := NewMiddleware(log)
			handler, err := opaMiddleware.GetHandler(test.meta)

			if test.shouldError != nil && *test.shouldError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			var reqCtx fh.RequestCtx
			if test.req != nil {
				test.req(&reqCtx)
			}
			handler(mockedRequestHandler)(&reqCtx)
			assert.Equal(t, test.status, reqCtx.Response.StatusCode())

			if test.headers != nil {
				for _, header := range *test.headers {
					assert.Equal(t, header[1], string(reqCtx.Response.Header.Peek(header[0])))
				}
			}
		})
	}
}
