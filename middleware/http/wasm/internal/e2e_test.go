/*
Copyright 2023 The Dapr Authors
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

package internal_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/components-contrib/middleware/http/wasm"
	"github.com/dapr/kit/logger"
)

var guestWasm map[string][]byte

const (
	guestWasmOutput  = "output"
	guestWasmRewrite = "rewrite"
	guestWasmConfig  = "config"
)

// TestMain ensures we can read the test wasm prior to running e2e tests.
func TestMain(m *testing.M) {
	wasms := []string{guestWasmOutput, guestWasmRewrite, guestWasmConfig}
	guestWasm = make(map[string][]byte, len(wasms))
	for _, name := range wasms {
		if wasm, err := os.ReadFile(path.Join("e2e-guests", name, "main.wasm")); err != nil {
			log.Panicln(err)
		} else {
			guestWasm[name] = wasm
		}
	}
	os.Exit(m.Run())
}

func Test_EndToEnd(t *testing.T) {
	l := logger.NewLogger(t.Name())
	var buf bytes.Buffer
	l.SetOutputLevel(logger.DebugLevel)
	l.SetOutput(&buf)

	type testCase struct {
		name     string
		guest    []byte
		property map[string]string
		test     func(t *testing.T, handler http.Handler, log *bytes.Buffer)
	}

	tests := []testCase{
		{
			name:  "consoleLog stdout and stderr",
			guest: guestWasm[guestWasmOutput],
			test: func(t *testing.T, handler http.Handler, log *bytes.Buffer) {
				var w http.ResponseWriter
				var r http.Request
				handler.ServeHTTP(w, &r)

				// First, we expect any console logging written inline from
				// init (main) and the request[0-9] funcs to info level.
				//
				// Then, we expect to see stdout and stderr from both scopes
				// at debug level.
				for _, s := range []string{
					`level=info msg="main ConsoleLog"`,
					`level=info msg="request[0] ConsoleLog"`,
					`level=debug msg="wasm stdout: main Stdout\nrequest[0] Stdout\n"`,
					`level=debug msg="wasm stderr: main Stderr\nrequest[0] Stderr\n"`,
				} {
					require.Contains(t, log.String(), s)
				}
			},
		},
		{
			name:  "consoleLog multiple requests",
			guest: guestWasm[guestWasmOutput],
			test: func(t *testing.T, handler http.Handler, log *bytes.Buffer) {
				// Service more requests than one to ensure pooling works properly.
				for range 3 {
					r := httptest.NewRequest(http.MethodGet, "/", nil)
					w := httptest.NewRecorder()
					handler.ServeHTTP(w, r)
				}

				// We expect to see initialization (main) twice, once for each
				// module in the pool. We expect to see request[1] which shows
				// round-robin back to the first module in the pool.
				for _, s := range []string{
					`level=info msg="main ConsoleLog"`,
					`level=info msg="request[0] ConsoleLog"`,
					`level=debug msg="wasm stdout: main Stdout\nrequest[0] Stdout\n"`,
					`level=debug msg="wasm stderr: main Stderr\nrequest[0] Stderr\n"`,
					`level=info msg="request[1] ConsoleLog"`,
					`level=debug msg="wasm stdout: request[1] Stdout\n"`,
					`level=debug msg="wasm stderr: request[1] Stderr\n"`,
				} {
					require.Contains(t, log.String(), s)
				}
			},
		},
		{
			name:  "rewrite",
			guest: guestWasm[guestWasmRewrite],
			test: func(t *testing.T, handler http.Handler, log *bytes.Buffer) {
				var w http.ResponseWriter
				u, err := url.Parse("https://test.io/v1.0/hi?name=panda")
				if err != nil {
					panic(err)
				}
				r := &http.Request{
					Method: http.MethodGet,
					URL:    u,
					Host:   "test.io",
					Header: map[string][]string{
						"User-Agent": {"curl/7.79.1"},
						"Accept":     {"*/*"},
					},
				}
				handler.ServeHTTP(w, r)

				require.Equal(t, "/v1.0/hello?name=teddy", r.URL.RequestURI())
			},
		},
		{
			name:     "log config to console",
			guest:    guestWasm[guestWasmConfig],
			property: map[string]string{"guestConfig": "config bytes in any format"},
			test: func(t *testing.T, handler http.Handler, log *bytes.Buffer) {
				require.Contains(t, log.String(), "config bytes in any format")
			},
		},
	}

	t.Run("local", func(t *testing.T) {
		for _, tt := range tests {
			tc := tt
			t.Run(tc.name, func(t *testing.T) {
				defer buf.Reset()

				wasmPath := path.Join(t.TempDir(), "guest.wasm")
				require.NoError(t, os.WriteFile(wasmPath, tc.guest, 0o600))

				meta := metadata.Base{Properties: map[string]string{"url": "file://" + wasmPath}}
				if len(tc.property) > 0 {
					for k, v := range tc.property {
						meta.Properties[k] = v
					}
				}
				handlerFn, err := wasm.NewMiddleware(l).GetHandler(context.Background(), middleware.Metadata{Base: meta})
				require.NoError(t, err)
				handler := handlerFn(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
				tc.test(t, handler, &buf)
			})
		}
	})

	httptests := []struct {
		name    string
		handler func(w http.ResponseWriter, r *http.Request, guest []byte)
	}{
		{
			name: "http",
			handler: func(w http.ResponseWriter, r *http.Request, guest []byte) {
				w.Write(guest)
			},
		},
		{
			name: "http+gzip",
			handler: func(w http.ResponseWriter, r *http.Request, guest []byte) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Content-Encoding", "gzip")

				gw := gzip.NewWriter(w)
				defer gw.Close()
				gw.Write(guest)
			},
		},
	}

	for _, ht := range httptests {
		t.Run(ht.name, func(t *testing.T) {
			for _, tt := range tests {
				tc := tt
				t.Run(tc.name, func(t *testing.T) {
					defer buf.Reset()
					ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						ht.handler(w, r, tc.guest)
					}))
					defer ts.Close()

					meta := metadata.Base{Properties: map[string]string{"url": ts.URL + "/guest.wasm"}}
					if len(tc.property) > 0 {
						for k, v := range tc.property {
							meta.Properties[k] = v
						}
					}
					handlerFn, err := wasm.NewMiddleware(l).GetHandler(context.Background(), middleware.Metadata{Base: meta})
					require.NoError(t, err)
					handler := handlerFn(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
					tc.test(t, handler, &buf)
				})
			}
		})
	}
}
