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

package routeralias

import (
	"testing"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"

	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
)

type RouterOutput struct{}

func (ro *RouterOutput) handle(ctx *fasthttp.RequestCtx) {
	ctx.Error(string(ctx.RequestURI()), fasthttp.StatusOK)
}

func TestRequestHandlerWithIllegalRouterRule(t *testing.T) {
	meta := middleware.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"/v1.0/mall/activity/info":      "/v1.0/invoke/srv.default/method/mall/activity/info",
				"/v1.0/hello/activity/:id/info": "/v1.0/invoke/srv.default/method/hello/activity/info",
				"/v1.0/hello/activity/:id/user": "/v1.0/invoke/srv.default/method/hello/activity/user",
			},
		},
	}
	log := logger.NewLogger("routeralias.test")
	ralias := NewMiddleware(log)
	handler, err := ralias.GetHandler(meta)
	assert.Nil(t, err)

	var ctx fasthttp.RequestCtx
	output := new(RouterOutput)
	ctx.Request.SetHost("localhost:5001")
	ctx.Request.Header.SetMethod("GET")

	t.Run("hit: change router with common request", func(t *testing.T) {
		ctx.Request.SetRequestURI("/v1.0/mall/activity/info?id=123")

		handler(output.handle)(&ctx)
		msg := string(ctx.Response.Body())
		assert.Equal(t, fasthttp.StatusOK, ctx.Response.Header.StatusCode())
		assert.Equal(t,
			"/v1.0/invoke/srv.default/method/mall/activity/info?id=123",
			msg)
	})

	t.Run("hit: change router with restful request", func(t *testing.T) {
		ctx.Request.SetRequestURI("/v1.0/hello/activity/1/info")

		output = new(RouterOutput)
		handler(output.handle)(&ctx)
		msg := string(ctx.Response.Body())
		assert.Equal(t, fasthttp.StatusOK, ctx.Response.Header.StatusCode())
		assert.Equal(t,
			"/v1.0/invoke/srv.default/method/hello/activity/info?id=1",
			msg)
	})

	t.Run("hit: change router with restful request and query string", func(t *testing.T) {
		ctx.Request.SetRequestURI("/v1.0/hello/activity/1/user?userid=123")

		output = new(RouterOutput)
		handler(output.handle)(&ctx)
		msg := string(ctx.Response.Body())
		assert.Equal(t, fasthttp.StatusOK, ctx.Response.Header.StatusCode())
		assert.Equal(t,
			"/v1.0/invoke/srv.default/method/hello/activity/user?id=1&userid=123",
			msg)
	})

	t.Run("miss: no change router", func(t *testing.T) {
		ctx.Request.SetRequestURI("/v1.0/invoke/srv.default")

		output = new(RouterOutput)
		handler(output.handle)(&ctx)
		msg := string(ctx.Response.Body())
		assert.Equal(t, fasthttp.StatusOK, ctx.Response.Header.StatusCode())
		assert.Equal(t,
			"/v1.0/invoke/srv.default",
			msg)
	})
}
