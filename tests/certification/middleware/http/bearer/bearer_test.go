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

package bearer_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	// Import the embed package.
	_ "embed"

	"github.com/dapr/components-contrib/middleware"
	bearerMw "github.com/dapr/components-contrib/middleware/http/bearer"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	httpMiddlewareLoader "github.com/dapr/dapr/pkg/components/middleware/http"
	httpMiddleware "github.com/dapr/dapr/pkg/middleware/http"
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/go-sdk/service/common"
	"github.com/dapr/kit/logger"
)

const (
	appID            = "myapp"
	invokeMethod     = "mymethod"
	tokenServicePort = 7470                                   // Defined in bearer.yaml
	tokenAudience    = "26b9502f-1336-4479-ad5b-c8366edb7206" // Defined in bearer.yaml
	tokenIssuer      = "http://localhost:7470"                // Defined in bearer.yaml
)

var (
	//go:embed jwks.json
	jwksData string
	//go:embed private-key.pem
	privateKeyPEM string
)

func TestHTTPMiddlewareBearer(t *testing.T) {
	var grpcPorts, httpPorts, appPorts [2]int
	client := http.Client{}

	for {
		ports, err := dapr_testing.GetFreePorts(6)
		require.NoError(t, err)

		// Ensure tokenServicePort isn't included
		if slices.Index(ports, tokenServicePort) > -1 {
			continue
		}

		grpcPorts = [2]int{ports[0], ports[3]}
		httpPorts = [2]int{ports[1], ports[4]}
		appPorts = [2]int{ports[2], ports[5]}
		break
	}

	// Load the private key
	privateKey, err := jwk.ParseKey([]byte(privateKeyPEM), jwk.WithPEM(true))
	require.NoError(t, err)

	requestsOpenIDConfiguration := atomic.Int32{}
	requestsJWKS := atomic.Int32{}

	// Step function that sets up a HTTP server that returns the JWKS
	setupJWKSServerStepFn := func() (string, flow.Runnable, flow.Runnable) {
		r := chi.NewRouter()

		r.Get("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
			requestsOpenIDConfiguration.Add(1)
			log.Println("Received request for OpenID Configuration document")
			w.Header().Set("content-type", "application/json")
			w.WriteHeader(http.StatusOK)

			res := map[string]string{
				"issuer":   tokenIssuer,
				"jwks_uri": tokenIssuer + "/.well-known/jwks.json",
			}
			json.NewEncoder(w).Encode(res)
		})

		r.Get("/.well-known/jwks.json", func(w http.ResponseWriter, r *http.Request) {
			requestsJWKS.Add(1)
			log.Println("Received request for JWKS")
			w.Header().Set("content-type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(jwksData))
		})

		srv := &http.Server{
			Addr:    fmt.Sprintf("127.0.0.1:%d", tokenServicePort),
			Handler: r,
		}

		start := flow.Runnable(func(ctx flow.Context) error {
			go func() {
				err := srv.ListenAndServe()
				if err != nil && !errors.Is(err, http.ErrServerClosed) {
					ctx.T.Fatalf("server error: %v", err)
				}
			}()

			time.Sleep(500 * time.Millisecond)
			return nil
		})

		stop := flow.Runnable(func(ctx flow.Context) error {
			shutdownCtx, shutdownCancel := context.WithTimeout(ctx, 1*time.Second)
			defer shutdownCancel()
			return srv.Shutdown(shutdownCtx)
		})

		return "start JWKS server", start, stop
	}

	type sendRequestOpts struct {
		AuthorizationHeader string
	}

	sendRequest := func(parentCtx context.Context, port int, opts *sendRequestOpts) (int, error) {
		invokeUrl := fmt.Sprintf("http://localhost:%d/v1.0/invoke/%s/method/%s", port, appID, invokeMethod)

		reqCtx, reqCancel := context.WithTimeout(parentCtx, 5*time.Second)
		defer reqCancel()

		req, err := http.NewRequestWithContext(reqCtx, "GET", invokeUrl, nil)
		if err != nil {
			return 0, fmt.Errorf("failed to create request: %w", err)
		}

		if opts != nil && opts.AuthorizationHeader != "" {
			req.Header.Set("authorization", opts.AuthorizationHeader)
		}

		res, err := client.Do(req)
		if err != nil {
			return 0, fmt.Errorf("request error: %w", err)
		}

		defer func() {
			// Drain before closing
			_, _ = io.Copy(io.Discard, res.Body)
			res.Body.Close()
		}()

		return res.StatusCode, nil
	}

	type runTestOpts struct {
		AuthFailure string // Options include: "empty"
	}

	// Run tests to check if rate-limiting is applied
	runBearerTest := func(opts runTestOpts) flow.Runnable {
		return func(ctx flow.Context) error {
			// Generate a new JWT
			now := time.Now()
			token, err := jwt.NewBuilder().
				Audience([]string{tokenAudience}).
				Issuer(tokenIssuer).
				IssuedAt(now).
				Expiration(now.Add(30 * time.Second)).
				Build()
			require.NoError(ctx.T, err)
			signedToken, err := jwt.Sign(token, jwt.WithKey(jwa.PS256, privateKey))
			require.NoError(ctx.T, err)

			var authHeader string
			switch opts.AuthFailure {
			case "empty":
				// Include nothing in the authorization header
				authHeader = ""
			case "lowercase":
				//  Include the generated token, but use "bearer" lowercase
				authHeader = "bearer " + string(signedToken)
			default:
				// Inlcude the generated token
				authHeader = "Bearer " + string(signedToken)
			}

			// Invoke both sidecars
			resStatus, err := sendRequest(ctx.Context, httpPorts[0], &sendRequestOpts{
				AuthorizationHeader: authHeader,
			})
			require.NoError(ctx.T, err)
			assert.Equal(t, http.StatusUnauthorized, resStatus)

			resStatus, err = sendRequest(ctx.Context, httpPorts[1], &sendRequestOpts{
				AuthorizationHeader: authHeader,
			})
			require.NoError(ctx.T, err)
			assert.Equal(t, http.StatusUnauthorized, resStatus)

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
	flow.New(t, "Bearer HTTP middleware").
		// Setup steps
		Step(setupJWKSServerStepFn()).
		// Start app and sidecar 1
		Step(app.Run("Start application 1", fmt.Sprintf(":%d", appPorts[0]), application)).
		Step(sidecar.Run(appID,
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPorts[0]),
			embedded.WithDaprGRPCPort(grpcPorts[0]),
			embedded.WithDaprHTTPPort(httpPorts[0]),
			embedded.WithResourcesPath("./resources"),
			embedded.WithAPILoggingEnabled(false),
			embedded.WithProfilingEnabled(false),
			componentRuntimeOptions(),
		)).
		// Start app and sidecar 2
		Step(app.Run("Start application 2", fmt.Sprintf(":%d", appPorts[1]), application)).
		Step(sidecar.Run(appID,
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPorts[1]),
			embedded.WithDaprGRPCPort(grpcPorts[1]),
			embedded.WithDaprHTTPPort(httpPorts[1]),
			embedded.WithResourcesPath("./resources"),
			embedded.WithAPILoggingEnabled(false),
			embedded.WithProfilingEnabled(false),
			componentRuntimeOptions(),
		)).
		// Tests
		Step("authorization header is empty", runBearerTest(runTestOpts{
			AuthFailure: "empty",
		})).
		// Run
		Run()
}

func componentRuntimeOptions() []runtime.Option {
	log := logger.NewLogger("dapr.components")
	log.SetOutputLevel(logger.DebugLevel)

	middlewareRegistry := httpMiddlewareLoader.NewRegistry()
	middlewareRegistry.Logger = log
	middlewareRegistry.RegisterComponent(func(log logger.Logger) httpMiddlewareLoader.FactoryMethod {
		return func(metadata middleware.Metadata) (httpMiddleware.Middleware, error) {
			return bearerMw.NewBearerMiddleware(log).GetHandler(context.Background(), metadata)
		}
	}, "bearer")

	return []runtime.Option{
		runtime.WithHTTPMiddlewares(middlewareRegistry),
	}
}
