package invoke

import (
	"testing"

	"github.com/dapr/components-contrib/middleware"
	"github.com/valyala/fasthttp"
)

var metadata = middleware.Metadata{
	Properties: map[string]string{
		"invokeURL":               "http://127.0.0.1:3500/v1.0/invoke/authorization/method/authorization",
		"invokeVerb":              "post",
		"enforceRequestVerbs":     "get,post,put,patch,delete",
		"timeout":                 "7",
		"insecureSkipVerify":      "true",
		"maxRetry":                "3",
		"expectedStatusCode":      "200",
		"forwardURLHeaderName":    "x-forward-url",
		"forwardMethodHeaderName": "x-forward-method",
	},
}

// Test Invoke Middleware GetHandler
func TestInvokeMiddlewareGetHandler(t *testing.T) {
	middleware := &Middleware{}

	tests := map[string]struct {
		req func(ctx *fasthttp.RequestCtx)
		ok  bool
	}{
		"no_enforced_request_verbs": {
			req: func(ctx *fasthttp.RequestCtx) {
				ctx.Request.Header.SetMethod("head")
			},
			ok: true,
		},
		"enforce_post_request_verbs": {
			req: func(ctx *fasthttp.RequestCtx) {
				ctx.Request.Header.SetMethod("post")
			},
			ok: true,
		},
	}

	handler, err := middleware.GetHandler(metadata)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range tests {
		// Construct new fasthttp.RequestCtx instance
		var reqCtx fasthttp.RequestCtx
		reqCtx.Request.SetHost("http://127.0.0.1:8000")
		reqCtx.Request.URI().SetPath("/authorization?q=1")
		reqCtx.Request.Header.Set("appid", "default")
		reqCtx.Request.Header.SetMethod("get")

		// Set fasthttp.RequestCtx
		v.req(&reqCtx)

		// mocked request handler
		called := false
		handler(func(ctx *fasthttp.RequestCtx) {
			called = true
		})(&reqCtx)

		if v.ok != called {
			t.Fatalf("%s: call clain got wrong", k)
		}
	}
}
