package routerchecker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

type RouterOutput struct{}

func (ro *RouterOutput) handle(ctx *fasthttp.RequestCtx) {
	ctx.Error(string(ctx.RequestURI()), fasthttp.StatusOK)
}

func TestRequestHandlerWithIllegalRouterRule(t *testing.T) {
	meta := middleware.Metadata{Properties: map[string]string{
		"rule": "^[A-Za-z0-9/._-]+$",
	}}
	log := logger.NewLogger("routerchecker.test")
	rchecker := NewMiddleware(log)
	handler, err := rchecker.GetHandler(meta)
	assert.Nil(t, err)

	var ctx fasthttp.RequestCtx
	ctx.Request.SetHost("localhost:5001")
	ctx.Request.SetRequestURI("/v1.0/invoke/qcg.default/method/ cat password")
	ctx.Request.Header.SetMethod("GET")

	output := new(RouterOutput)
	handler(output.handle)(&ctx)
	assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.Header.StatusCode())
}

func TestRequestHandlerWithLegalRouterRule(t *testing.T) {
	meta := middleware.Metadata{Properties: map[string]string{
		"rule": "^[A-Za-z0-9/._-]+$",
	}}

	log := logger.NewLogger("routerchecker.test")
	rchecker := NewMiddleware(log)
	handler, err := rchecker.GetHandler(meta)
	assert.Nil(t, err)

	var ctx fasthttp.RequestCtx
	ctx.Request.SetHost("localhost:5001")
	ctx.Request.SetRequestURI("/v1.0/invoke/qcg.default/method")
	ctx.Request.Header.SetMethod("GET")

	output := new(RouterOutput)
	handler(output.handle)(&ctx)
	assert.Equal(t, fasthttp.StatusOK, ctx.Response.Header.StatusCode())
}
