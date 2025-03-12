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

package opa_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/dapr/components-contrib/middleware"
	opaMw "github.com/dapr/components-contrib/middleware/http/opa"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	httpMiddlewareLoader "github.com/dapr/dapr/pkg/components/middleware/http"
	"github.com/dapr/dapr/pkg/config/protocol"
	runtimeMiddleware "github.com/dapr/dapr/pkg/middleware"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/go-sdk/service/common"
	"github.com/dapr/kit/logger"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

const (
	appID              = "myapp"
	invokeMethod       = "mymethod"
	responseHeaderName = "x-custom-header"
)

// Logger
var log = logger.NewLogger("dapr.components")

func TestHTTPMiddlewareOpa(t *testing.T) {
	ports, err := dapr_testing.GetFreePorts(6)
	require.NoError(t, err)

	grpcPorts := [2]int{ports[0], ports[3]}
	httpPorts := [2]int{ports[1], ports[4]}
	appPorts := [2]int{ports[2], ports[5]}

	client := http.Client{}

	// Application setup code
	application := func(ctx flow.Context, s common.Service) error {
		s.AddServiceInvocationHandler(invokeMethod, func(ctx context.Context, in *common.InvocationEvent) (out *common.Content, err error) {
			return &common.Content{}, nil
		})
		return nil
	}

	type sendRequestOpts struct {
		Body string
	}

	sendRequest := func(parentCtx context.Context, port int, opts *sendRequestOpts) (int, string, error) {
		invokeUrl := fmt.Sprintf("http://localhost:%d/v1.0/invoke/%s/method/%s", port, appID, invokeMethod)

		reqCtx, reqCancel := context.WithTimeout(parentCtx, 150*time.Second)
		// reqCtx, reqCancel := context.WithTimeout(parentCtx, 5*time.Second)
		defer reqCancel()

		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, invokeUrl, bytes.NewBufferString(opts.Body))
		if err != nil {
			return 0, "", fmt.Errorf("failed to create request: %w", err)
		}

		res, err := client.Do(req)
		if err != nil {
			return 0, "", fmt.Errorf("request error: %w", err)
		}

		defer func() {
			// Drain before closing
			_, _ = io.Copy(io.Discard, res.Body)
			res.Body.Close()
		}()

		return res.StatusCode, res.Header.Get(responseHeaderName), nil
	}

	// Run tests to check if the bearer token is validated correctly
	opaTests := func(ctx flow.Context) error {
		tests := []struct {
			name                string
			body                string
			statusCode          int
			expectedHeaderValue string
		}{
			{
				name:                "default response",
				body:                "",
				statusCode:          http.StatusTeapot,
				expectedHeaderValue: "",
			}, {
				name:                "allowed request",
				body:                "allow-ok",
				statusCode:          http.StatusOK,
				expectedHeaderValue: "",
			}, {
				name:                "redirected",
				body:                "redirect",
				statusCode:          http.StatusMovedPermanently,
				expectedHeaderValue: "redirected",
			},
		}

		ctx.T.Run("bearer token validation", func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					// Invoke sidecar
					resStatus, resHeaderValue, err := sendRequest(ctx.Context, httpPorts[0], &sendRequestOpts{
						Body: tt.body,
					})
					require.NoError(t, err)
					assert.Equal(t, tt.statusCode, resStatus)
					assert.Equal(t, tt.expectedHeaderValue, resHeaderValue)
				})
			}
		})

		return nil
	}

	flow.New(t, "OPA HTTP Middleware").
		// Start app and sidecar
		Step(app.Run("Start application", fmt.Sprintf(":%d", appPorts[0]), application)).
		Step(sidecar.Run(appID,
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPorts[0])),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPorts[0])),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPorts[0])),
				embedded.WithResourcesPath("./resources"),
				embedded.WithAPILoggingEnabled(true),
				embedded.WithProfilingEnabled(false),
			)...,
		)).
		// Tests
		Step("opa validation", opaTests).
		// Run
		Run()
}

func componentRuntimeOptions() []embedded.Option {
	middlewareRegistry := httpMiddlewareLoader.NewRegistry()
	middlewareRegistry.Logger = log
	middlewareRegistry.RegisterComponent(func(log logger.Logger) httpMiddlewareLoader.FactoryMethod {
		return func(metadata middleware.Metadata) (runtimeMiddleware.HTTP, error) {
			return opaMw.NewMiddleware(log).GetHandler(context.Background(), metadata)
		}
	}, "opa")

	return []embedded.Option{
		embedded.WithHTTPMiddlewares(middlewareRegistry),
	}
}
