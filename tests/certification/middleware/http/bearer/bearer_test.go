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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
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

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/middleware"
	bearerMw "github.com/dapr/components-contrib/middleware/http/bearer"
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
	appID            = "myapp"
	invokeMethod     = "mymethod"
	tokenServicePort = 7470                                   // Defined in bearer.yaml
	tokenAudience    = "26b9502f-1336-4479-ad5b-c8366edb7206" // Defined in bearer.yaml
	tokenIssuer      = "http://localhost:7470"                // Defined in bearer.yaml
)

var (
	//go:embed jwks.json
	jwksData string
	//go:embed private.json
	privateKeyData string

	// Logger
	log = logger.NewLogger("dapr.components")
)

func init() {
	log.SetOutputLevel(logger.DebugLevel)
}

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

	// Load the private keys
	privateKeys, err := jwk.Parse([]byte(privateKeyData))
	require.NoError(t, err)

	// Counters for requests coming to the web server
	requestsOpenIDConfiguration := atomic.Int32{}
	requestsJWKS := atomic.Int32{}

	// Step function that sets up a HTTP server that returns the JWKS
	setupJWKSServerStepFn := func() (string, flow.Runnable, flow.Runnable) {
		r := chi.NewRouter()

		openIDConfigurationHandler := func(w http.ResponseWriter, r *http.Request) {
			requestsOpenIDConfiguration.Add(1)
			log.Info("Received request for OpenID Configuration document")
			w.Header().Set("content-type", "application/json")
			w.WriteHeader(http.StatusOK)

			res := map[string]string{
				"issuer":   tokenIssuer,
				"jwks_uri": tokenIssuer + "/.well-known/jwks.json",
			}
			json.NewEncoder(w).Encode(res)
		}
		r.Get("/.well-known/openid-configuration", openIDConfigurationHandler)
		r.Get("/foo/.well-known/openid-configuration", openIDConfigurationHandler)

		r.Get("/.well-known/jwks.json", func(w http.ResponseWriter, r *http.Request) {
			requestsJWKS.Add(1)
			log.Info("Received request for JWKS")
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

		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, invokeUrl, nil)
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

	// Run tests to check if the bearer token is validated correctly
	bearerTests := func(ctx flow.Context) error {
		now := time.Now()

		tests := []struct {
			name         string
			buildTokenFn func(builder *jwt.Builder)
			signTokenFn  func(builder *jwt.Builder) ([]byte, error)
			authHeaderFn func(token string) string
			signingKeyID int
			statusCode   int
		}{
			{
				name:       "valid auth header",
				statusCode: http.StatusOK,
			},
			{
				name:       "lowercase bearer in token",
				statusCode: http.StatusOK,
				authHeaderFn: func(token string) string {
					return "bearer " + token
				},
			},
			{
				name:       "empty authorization header",
				statusCode: http.StatusUnauthorized,
				authHeaderFn: func(token string) string {
					return ""
				},
			},
			{
				name:       "empty bearer token 1",
				statusCode: http.StatusUnauthorized,
				authHeaderFn: func(token string) string {
					return "Bearer"
				},
			},
			{
				name:       "empty bearer token 2",
				statusCode: http.StatusUnauthorized,
				authHeaderFn: func(token string) string {
					return "Bearer "
				},
			},
			{
				name:       "malformed JWT",
				statusCode: http.StatusUnauthorized,
				authHeaderFn: func(token string) string {
					return "Bearer iMjZiOTUwMmYtMTMzN"
				},
			},
			{
				name:       "expired token",
				statusCode: http.StatusUnauthorized,
				buildTokenFn: func(builder *jwt.Builder) {
					builder.IssuedAt(now.Add(-20 * time.Minute))
					builder.Expiration(now.Add(-10 * time.Minute))
				},
			},
			{
				name:       "token but within allowed clock skew",
				statusCode: http.StatusOK,
				buildTokenFn: func(builder *jwt.Builder) {
					builder.IssuedAt(now.Add(-20 * time.Minute))
					builder.Expiration(now.Add(-1 * time.Minute))
				},
			},
			{
				name:       "token not yet valid",
				statusCode: http.StatusUnauthorized,
				buildTokenFn: func(builder *jwt.Builder) {
					builder.NotBefore(now.Add(20 * time.Minute))
					builder.IssuedAt(now.Add(20 * time.Minute))
				},
			},
			{
				name:       "token not yet valid but within allowed clock skew",
				statusCode: http.StatusOK,
				buildTokenFn: func(builder *jwt.Builder) {
					builder.NotBefore(now.Add(1 * time.Minute))
					builder.IssuedAt(now.Add(1 * time.Minute))
				},
			},
			{
				name:       "invalid token audience",
				statusCode: http.StatusUnauthorized,
				buildTokenFn: func(builder *jwt.Builder) {
					builder.Audience([]string{"foo"})
				},
			},
			{
				name:       "empty token audience",
				statusCode: http.StatusUnauthorized,
				buildTokenFn: func(builder *jwt.Builder) {
					builder.Audience([]string{})
				},
			},
			{
				name:       "invalid token issuer",
				statusCode: http.StatusUnauthorized,
				buildTokenFn: func(builder *jwt.Builder) {
					builder.Issuer("foo")
				},
			},
			{
				name:       "empty token issuer",
				statusCode: http.StatusUnauthorized,
				buildTokenFn: func(builder *jwt.Builder) {
					builder.Issuer("")
				},
			},
			{
				name:       "reject tokens with alg 'none'",
				statusCode: http.StatusUnauthorized,
				signTokenFn: func(builder *jwt.Builder) ([]byte, error) {
					// {"alg":"none"}
					const joseHeader = `eyJhbGciOiJub25lIn0`
					token, err := builder.Build()
					if err != nil {
						return nil, err
					}

					claimSet, err := jwt.NewSerializer().Serialize(token)
					if err != nil {
						return nil, err
					}

					return []byte(joseHeader + "." + base64.RawURLEncoding.EncodeToString(claimSet) + "."), nil
				},
			},
			{
				name:         "token signed with wrong key",
				statusCode:   http.StatusUnauthorized,
				signingKeyID: 1,
			},
		}

		ctx.T.Run("bearer token validation", func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					// Generate a new JWT
					builder := jwt.NewBuilder().
						Audience([]string{tokenAudience}).
						Issuer(tokenIssuer).
						IssuedAt(now).
						Expiration(now.Add(2 * time.Minute))

					// If we have a tokenFn, invoke that
					if tt.buildTokenFn != nil {
						tt.buildTokenFn(builder)
					}

					// Build the token
					// If we have a signTokenFn, invoke that
					var signedToken []byte
					if tt.signTokenFn != nil {
						signedToken, err = tt.signTokenFn(builder)
						require.NoError(t, err)
					} else {
						token, err := builder.Build()
						require.NoError(t, err)
						useKey, _ := privateKeys.Key(tt.signingKeyID)
						signedToken, err = jwt.Sign(token, jwt.WithKey(jwa.PS256, useKey))
						require.NoError(t, err)
					}

					// Set the auth header
					var authHeader string
					if tt.authHeaderFn != nil {
						authHeader = tt.authHeaderFn(string(signedToken))
					} else {
						authHeader = "Bearer " + string(signedToken)
					}

					// Invoke both sidecars
					resStatus, err := sendRequest(ctx.Context, httpPorts[0], &sendRequestOpts{
						AuthorizationHeader: authHeader,
					})
					require.NoError(t, err)
					assert.Equal(t, tt.statusCode, resStatus)

					resStatus, err = sendRequest(ctx.Context, httpPorts[1], &sendRequestOpts{
						AuthorizationHeader: authHeader,
					})
					require.NoError(t, err)
					assert.Equal(t, tt.statusCode, resStatus)
				})
			}
		})

		return nil
	}

	// Run tests to validate component initialization
	initializationTests := func(ctx flow.Context) error {
		ctx.T.Run("component initialization", func(t *testing.T) {
			initMiddleware := func(md map[string]string) error {
				_, err := bearerMw.
					NewBearerMiddleware(log).
					GetHandler(context.Background(), middleware.Metadata{Base: metadata.Base{
						Name:       "test",
						Properties: md,
					}})
				return err
			}

			t.Run("successful initialization", func(t *testing.T) {
				curRequestsOpenIDConfiguration := requestsOpenIDConfiguration.Load()
				curRequestsJWKS := requestsJWKS.Load()

				err := initMiddleware(map[string]string{
					"issuer":   tokenIssuer,
					"audience": tokenAudience,
				})
				require.NoError(t, err)

				// Both endpoints should be requested
				assert.Equal(t, curRequestsOpenIDConfiguration+1, requestsOpenIDConfiguration.Load())
				assert.Equal(t, curRequestsJWKS+1, requestsJWKS.Load())
			})

			t.Run("explicit JWKS URL", func(t *testing.T) {
				curRequestsOpenIDConfiguration := requestsOpenIDConfiguration.Load()
				curRequestsJWKS := requestsJWKS.Load()

				err := initMiddleware(map[string]string{
					"issuer":   tokenIssuer,
					"audience": tokenAudience,
					"jwksURL":  tokenIssuer + "/.well-known/jwks.json",
				})
				require.NoError(t, err)

				// Only the JWKS endpoint should be requested
				assert.Equal(t, curRequestsOpenIDConfiguration, requestsOpenIDConfiguration.Load())
				assert.Equal(t, curRequestsJWKS+1, requestsJWKS.Load())
			})

			t.Run("cannot find OpenID configuration document", func(t *testing.T) {
				curRequestsOpenIDConfiguration := requestsOpenIDConfiguration.Load()
				curRequestsJWKS := requestsJWKS.Load()

				err := initMiddleware(map[string]string{
					"issuer":   tokenIssuer + "/notfound",
					"audience": tokenAudience,
				})
				require.Error(t, err)
				assert.ErrorContains(t, err, "invalid response status code: 404")

				// No endpoint should be requested
				assert.Equal(t, curRequestsOpenIDConfiguration, requestsOpenIDConfiguration.Load())
				assert.Equal(t, curRequestsJWKS, requestsJWKS.Load())
			})

			t.Run("token issuer mismatch in OpenID configuration document", func(t *testing.T) {
				curRequestsOpenIDConfiguration := requestsOpenIDConfiguration.Load()
				curRequestsJWKS := requestsJWKS.Load()

				err := initMiddleware(map[string]string{
					"issuer":   tokenIssuer + "/foo",
					"audience": tokenAudience,
				})
				require.Error(t, err)
				assert.ErrorContains(t, err, "the issuer found in the OpenID Configuration document")

				// Only the OpenID Configuration endpoint should be requested
				assert.Equal(t, curRequestsOpenIDConfiguration+1, requestsOpenIDConfiguration.Load())
				assert.Equal(t, curRequestsJWKS, requestsJWKS.Load())
			})

			t.Run("cannot find JWKS", func(t *testing.T) {
				curRequestsOpenIDConfiguration := requestsOpenIDConfiguration.Load()
				curRequestsJWKS := requestsJWKS.Load()

				err := initMiddleware(map[string]string{
					"issuer":   tokenIssuer,
					"audience": tokenAudience,
					"jwksURL":  tokenIssuer + "/not-found/jwks.json",
				})
				require.Error(t, err)
				assert.ErrorContains(t, err, "failed to fetch JWKS")

				// No endpoint should be requested
				assert.Equal(t, curRequestsOpenIDConfiguration, requestsOpenIDConfiguration.Load())
				assert.Equal(t, curRequestsJWKS, requestsJWKS.Load())
			})
		})

		return nil
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
		Step("bearer token validation", bearerTests).
		Step("component initialization", initializationTests).
		// Run
		Run()
}

func componentRuntimeOptions() []embedded.Option {
	middlewareRegistry := httpMiddlewareLoader.NewRegistry()
	middlewareRegistry.Logger = log
	middlewareRegistry.RegisterComponent(func(log logger.Logger) httpMiddlewareLoader.FactoryMethod {
		return func(metadata middleware.Metadata) (httpMiddleware.Middleware, error) {
			return bearerMw.NewBearerMiddleware(log).GetHandler(context.Background(), metadata)
		}
	}, "bearer")

	return []embedded.Option{
		embedded.WithHTTPMiddlewares(middlewareRegistry),
	}
}
