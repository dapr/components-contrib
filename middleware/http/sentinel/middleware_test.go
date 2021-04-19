package sentinel

import (
	"testing"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
)

type counter struct {
	count int32
}

func (c *counter) handle(ctx *fasthttp.RequestCtx) {
	c.count++
}

func TestRequestHandlerWithFlowRules(t *testing.T) {
	meta := middleware.Metadata{Properties: map[string]string{
		"appName": "test-app",
		"flowRules": `[
	{
		"resource": "GET:/v1.0/nodeapp/healthz",
		"threshold": 10,
		"tokenCalculateStrategy": 0,
		"controlBehavior": 0
	}
]`,
	}}

	log := logger.NewLogger("sentinel.test")
	sentinel := NewMiddleware(log)
	handler, err := sentinel.GetHandler(meta)
	assert.Nil(t, err)

	var ctx fasthttp.RequestCtx
	ctx.Request.SetHost("localhost:5001")
	ctx.Request.SetRequestURI("/v1.0/nodeapp/healthz")
	ctx.Request.Header.SetMethod("GET")

	counter := &counter{}
	for i := 0; i < 100; i++ {
		handler(counter.handle)(&ctx)
	}

	assert.Equal(t, int32(10), counter.count)
}
