package routerchecker

import (
	"testing"

	"github.com/dapr/components-contrib/middleware"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
)

type RouterOutput struct{}

func (ro *RouterOutput) handle(ctx *fasthttp.RequestCtx) {
	ctx.Error(string(ctx.RequestURI()), fasthttp.StatusOK)
}

func TestIsMatchRule(t *testing.T) {
	var m = new(Middleware)
	isMatch, err := m.isMatchRule("[\\s]", "/v1.0/invoke/qcg.default/method/ cat password")
	assert.Nil(t, err, "is nil")
	assert.Equal(t, true, isMatch)
	isMatch, err = m.isMatchRule("[\\s]", "/v1.0/invoke/qcg.default/method/sss")
	assert.Nil(t, err, "is nil")
	assert.Equal(t, false, isMatch)
}

func TestRequestHandlerWithIllegalRouterRule(t *testing.T) {
	meta := middleware.Metadata{Properties: map[string]string{
		"rule": "[\\s]",
	}}
	rchecker := NewRouterCheckerMiddleware()
	handler, err := rchecker.GetHandler(meta)
	assert.Nil(t, err)

	var ctx fasthttp.RequestCtx
	ctx.Request.SetHost("localhost:5001")
	ctx.Request.SetRequestURI("/v1.0/invoke/qcg.default/method/ cat password")
	ctx.Request.Header.SetMethod("GET")

	var output = new(RouterOutput)
	handler(output.handle)(&ctx)
	assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.Header.StatusCode())
}

func TestRequestHandlerWithLegalRouterRule(t *testing.T) {
	meta := middleware.Metadata{Properties: map[string]string{
		"rule": "[\\s]",
	}}
	rchecker := NewRouterCheckerMiddleware()
	handler, err := rchecker.GetHandler(meta)
	assert.Nil(t, err)

	var ctx fasthttp.RequestCtx
	ctx.Request.SetHost("localhost:5001")
	ctx.Request.SetRequestURI("/v1.0/invoke/qcg.default/method")
	ctx.Request.Header.SetMethod("GET")

	var output = new(RouterOutput)
	handler(output.handle)(&ctx)
	assert.Equal(t, fasthttp.StatusOK, ctx.Response.Header.StatusCode())
}
