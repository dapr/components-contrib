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

package ratelimit_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/ratelimit"

	"github.com/dapr/components-contrib/middleware"
	ratelimitMw "github.com/dapr/components-contrib/middleware/http/ratelimit"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	httpMiddlewareLoader "github.com/dapr/dapr/pkg/components/middleware/http"
	"github.com/dapr/dapr/pkg/config/protocol"
	httpMiddleware "github.com/dapr/dapr/pkg/middleware/http"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/go-sdk/service/common"
	"github.com/dapr/kit/logger"
)

const (
	appID        = "myapp"
	invokeMethod = "mymethod"

	// RPS limit as defined in the component's spec
	limitRps = 10
)

func TestHTTPMiddlewareRatelimit(t *testing.T) {
	ports, err := dapr_testing.GetFreePorts(6)
	require.NoError(t, err)

	grpcPorts := [2]int{ports[0], ports[3]}
	httpPorts := [2]int{ports[1], ports[4]}
	appPorts := [2]int{ports[2], ports[5]}

	client := http.Client{}

	type sendRequestOpts struct {
		SourceIPHeader string
	}

	sendRequest := func(parentCtx context.Context, port int, opts *sendRequestOpts) (bool, error) {
		invokeUrl := fmt.Sprintf("http://localhost:%d/v1.0/invoke/%s/method/%s", port, appID, invokeMethod)

		reqCtx, reqCancel := context.WithTimeout(parentCtx, 5*time.Second)
		defer reqCancel()

		req, err := http.NewRequestWithContext(reqCtx, "GET", invokeUrl, nil)
		if err != nil {
			return false, fmt.Errorf("failed to create request: %w", err)
		}

		if opts != nil && opts.SourceIPHeader != "" {
			req.Header.Set("X-Forwarded-For", opts.SourceIPHeader)
		}

		res, err := client.Do(req)
		if err != nil {
			return false, fmt.Errorf("request error: %w", err)
		}

		defer func() {
			// Drain before closing
			_, _ = io.Copy(io.Discard, res.Body)
			res.Body.Close()
		}()

		return res.StatusCode == http.StatusOK, nil
	}

	type rateLimitTestOpts struct {
		rps           int
		reqsPerThread int
		threads       int
		sidecars      int
		sourceIPs     []string
	}

	// Run tests to check if rate-limiting is applied
	rateLimitTest := func(opts rateLimitTestOpts) flow.Runnable {
		return func(ctx flow.Context) error {
			limiter := ratelimit.New(opts.rps)

			wg := sync.WaitGroup{}
			passed := atomic.Uint32{}
			failed := atomic.Uint32{}
			start := time.Now()

			// Run multiple goroutines in parallel
			for i := 0; i < opts.threads; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					sro := &sendRequestOpts{}

					for j := 0; j < opts.reqsPerThread; j++ {
						limiter.Take()

						if len(opts.sourceIPs) > 0 {
							sro.SourceIPHeader = opts.sourceIPs[j%len(opts.sourceIPs)]
						}

						ok, err := sendRequest(ctx.Context, httpPorts[j%opts.sidecars], sro)
						require.NoError(ctx.T, err)
						if ok {
							passed.Add(1)
						} else {
							failed.Add(1)
						}
					}
				}()
			}
			wg.Wait()

			duration := time.Since(start)

			multiplier := uint32(opts.sidecars)
			if len(opts.sourceIPs) > 0 {
				multiplier *= uint32(len(opts.sourceIPs))
			}

			// If the rps is less than limitRps (per sidecar per source IP), we expect no failure
			if uint32(opts.rps) < (limitRps * multiplier) {
				ctx.T.Logf("Test duration: %v. Passed requests: %d. Failed requests: %d", duration, passed.Load(), failed.Load())
				require.Equal(ctx.T, uint32(opts.threads*opts.reqsPerThread), passed.Load()+failed.Load())
				assert.Equal(ctx.T, uint32(0), failed.Load())
			} else {
				// Depending on the duration of the test, we should have approximately limitRps rps (per sidecar per source IP)
				// We also add a 50% buffer to account for allowed bursting and for variations during tests
				expected := uint32(duration.Seconds()*limitRps*1.5) * multiplier

				ctx.T.Logf("Test duration: %v. Passed requests: %d (expected less than: %d). Failed requests: %d", duration, passed.Load(), expected, failed.Load())
				require.Equal(ctx.T, uint32(opts.threads*opts.reqsPerThread), passed.Load()+failed.Load())
				assert.Less(ctx.T, passed.Load(), expected)
				assert.Greater(ctx.T, failed.Load(), uint32(0))
			}

			return nil
		}
	}

	// Application setup code
	application := func(ctx flow.Context, s common.Service) error {
		s.AddServiceInvocationHandler(invokeMethod, func(ctx context.Context, in *common.InvocationEvent) (out *common.Content, err error) {
			return &common.Content{}, nil
		})
		return nil
	}

	// Run tests
	flow.New(t, "Rate-limiter test").
		// Start app and sidecar 1
		Step(app.Run("Start application 1", fmt.Sprintf(":%d", appPorts[0]), application)).
		Step(sidecar.Run(appID,
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPorts[0])),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPorts[0])),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPorts[0])),
				embedded.WithResourcesPath("./resources"),
				embedded.WithAPILoggingEnabled(false),
				embedded.WithProfilingEnabled(false),
			)...,
		)).
		// Start app and sidecar 2
		Step(app.Run("Start application 2", fmt.Sprintf(":%d", appPorts[1]), application)).
		Step(sidecar.Run(appID,
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPorts[1])),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPorts[1])),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPorts[1])),
				embedded.WithResourcesPath("./resources"),
				embedded.WithAPILoggingEnabled(false),
				embedded.WithProfilingEnabled(false),
			)...,
		)).
		// Tests
		Step("Single sidecar, requests below rate-limit", rateLimitTest(rateLimitTestOpts{
			rps:           5,
			reqsPerThread: 15,
			threads:       5,
			sidecars:      1,
		})).
		Step("Single sidecar, requests above rate-limit", rateLimitTest(rateLimitTestOpts{
			rps:           20,
			reqsPerThread: 20,
			threads:       10,
			sidecars:      1,
		})).
		Step("Rate-limiting is applied per-IP", rateLimitTest(rateLimitTestOpts{
			reqsPerThread: 20,
			threads:       10,
			rps:           30,
			sidecars:      1,
			sourceIPs:     []string{"1.2.3.4", "5.6.7.8"},
		})).
		Step("Multiple sidecars, requests below rate-limit", rateLimitTest(rateLimitTestOpts{
			rps:           10,
			reqsPerThread: 20,
			threads:       5,
			sidecars:      2,
		})).
		Step("Multiple sidecars, requests above rate-limit", rateLimitTest(rateLimitTestOpts{
			rps:           50,
			reqsPerThread: 20,
			threads:       10,
			sidecars:      2,
		})).
		// Run
		Run()

}

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")

	middlewareRegistry := httpMiddlewareLoader.NewRegistry()
	middlewareRegistry.Logger = log
	middlewareRegistry.RegisterComponent(func(log logger.Logger) httpMiddlewareLoader.FactoryMethod {
		return func(metadata middleware.Metadata) (httpMiddleware.Middleware, error) {
			return ratelimitMw.NewRateLimitMiddleware(log).GetHandler(context.Background(), metadata)
		}
	}, "ratelimit")

	return []embedded.Option{
		embedded.WithHTTPMiddlewares(middlewareRegistry),
	}
}
