package sentinel

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
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

func TestLoadRules(t *testing.T) {
	cases := []struct {
		name      string
		meta      middlewareMetadata
		expectErr bool
	}{
		{
			name: "Invalid flow rules but return no error",
			meta: middlewareMetadata{
				AppName: "nodeapp",
				FlowRules: `[
	{
		"resource": "GET:/v1.0/nodeapp/healthz",
		"strategy": 1,
		"statIntervalInMs": -1 
	}
]`,
			},
			expectErr: true,
		},
		{
			name: "Invalid circuit breaker rules and return error",
			meta: middlewareMetadata{
				AppName: "nodeapp",
				CircuitBreakerRules: `[
	{
		"resource": "GET:/v1.0/nodeapp/healthz",
		"strategy": 1,
		"not-existing-property": -1 
	}
]`,
			},
			expectErr: false,
		},
		{
			name: "Invalid hotspot rules and return no error",
			meta: middlewareMetadata{
				AppName: "nodeapp",
				HotSpotParamRules: `[
	{
		"resource": "GET:/v1.0/nodeapp/healthz",
		"metricType": 1,
		"not-existing-property": -1 
	}
]`,
			},
			expectErr: false,
		},
		{
			name: "Invalid system rules and return no error",
			meta: middlewareMetadata{
				AppName: "nodeapp",
				SystemRules: `[
	{
	}
]`,
			},
			expectErr: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sentinel := NewMiddleware(nil)
			err := sentinel.loadSentinelRules(&c.meta)
			if c.expectErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}
