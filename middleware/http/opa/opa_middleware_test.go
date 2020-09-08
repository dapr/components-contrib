package opa

import (
	"strings"
	"testing"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	fh "github.com/valyala/fasthttp"
)

func mockedRequestHandler(ctx *fh.RequestCtx) {}

func TestBasicOpaPolicy(t *testing.T) {
	var metadata middleware.Metadata
	metadata.Properties = map[string]string{
		"rego": `package http
allow = true`,
	}

	log := logger.NewLogger("opa.test")
	opaMiddleware := NewOpaMiddleware(log)
	handler, err := opaMiddleware.GetHandler(metadata)
	require.NoError(t, err)

	var reqCtx fh.RequestCtx
	handler(mockedRequestHandler)(&reqCtx)
	assert.NotEqual(t, 401, reqCtx.Response.StatusCode())
}

func TestNotAllowed(t *testing.T) {
	var metadata middleware.Metadata
	metadata.Properties = map[string]string{
		"rego": `package http
allow = false`,
	}

	log := logger.NewLogger("opa.test")
	opaMiddleware := NewOpaMiddleware(log)
	handler, err := opaMiddleware.GetHandler(metadata)
	require.NoError(t, err)

	var reqCtx fh.RequestCtx
	handler(mockedRequestHandler)(&reqCtx)
	assert.Equal(t, 401, reqCtx.Response.StatusCode())
}

func TestSetStatusCode(t *testing.T) {
	var metadata middleware.Metadata
	metadata.Properties = map[string]string{
		"rego": strings.TrimSpace(`
package http
allow = {
	"allow": false,
	"status_code": 301
}`),
	}

	log := logger.NewLogger("opa.test")
	opaMiddleware := NewOpaMiddleware(log)
	handler, err := opaMiddleware.GetHandler(metadata)
	require.NoError(t, err)

	var reqCtx fh.RequestCtx
	handler(mockedRequestHandler)(&reqCtx)
	assert.Equal(t, 301, reqCtx.Response.StatusCode())
}

func TestAddResponseHeaders(t *testing.T) {
	var metadata middleware.Metadata
	metadata.Properties = map[string]string{
		"rego": strings.TrimSpace(`
package http
allow = {
	"allow": false,
	"status_code": 301,
	"additional_headers": { "location": "https://my.site/login" }
}`),
	}

	log := logger.NewLogger("opa.test")
	opaMiddleware := NewOpaMiddleware(log)
	handler, err := opaMiddleware.GetHandler(metadata)
	require.NoError(t, err)

	var reqCtx fh.RequestCtx
	handler(mockedRequestHandler)(&reqCtx)
	assert.Equal(t, 301, reqCtx.Response.StatusCode())
	assert.Equal(t, "https://my.site/login", string(reqCtx.Response.Header.Peek("location")))
}

func TestAddRequestHeaders(t *testing.T) {
	var metadata middleware.Metadata
	metadata.Properties = map[string]string{
		"rego": strings.TrimSpace(`
package http
allow = {
	"allow": true,
	"additional_headers": { "x-key": "abc" }
}`),
	}

	log := logger.NewLogger("opa.test")
	opaMiddleware := NewOpaMiddleware(log)
	handler, err := opaMiddleware.GetHandler(metadata)
	require.NoError(t, err)

	var reqCtx fh.RequestCtx
	handler(mockedRequestHandler)(&reqCtx)
	assert.Equal(t, "abc", string(reqCtx.Request.Header.Peek("x-key")))
}
