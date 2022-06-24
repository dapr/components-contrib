package basic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

type testHandler struct{}

func (t *testHandler) handle(ctx *fasthttp.RequestCtx) {
}

func TestGetHandler(t *testing.T) {
	meta := middleware.Metadata{Properties: map[string]string{
		"path":    "./hello.wasm",
		"runtime": "wazero",
	}}
	log := logger.NewLogger("wasm.basic.test")
	handler, err := NewMiddleware(log).GetHandler(meta)
	assert.Nil(t, err)

	var ctx fasthttp.RequestCtx
	ctx.Request.SetRequestURI("/v1.0/hi")

	th := &testHandler{}
	handler(th.handle)(&ctx)

	assert.Equal(t, "/v1.0/hello", string(ctx.RequestURI()))
}
