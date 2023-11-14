/*
Copyright 2023 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implieout.
See the License for the specific language governing permissions and
limitations under the License.
*/

package wasm

import (
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var wasmMagicNumber = []byte{0x00, 0x61, 0x73, 0x6d}

func TestWasmHTTPFetch(t *testing.T) {
	wasmBinary := wasmMagicNumber
	wasmBinary = append(wasmBinary, 0x00, 0x00, 0x00, 0x00)
	cases := []struct {
		name          string
		handler       http.HandlerFunc
		expectedError string
	}{
		{
			name: "plain wasm binary",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Write(wasmBinary)
			},
		},
		// Compressed payloads are handled automatically by http.Client.
		{
			name: "compressed payload",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Content-Encoding", "gzip")

				gw := gzip.NewWriter(w)
				defer gw.Close()
				gw.Write(wasmBinary)
			},
		},
		{
			name: "http error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
			expectedError: "received 500 status code",
		},
	}

	for _, proto := range []string{"http", "https"} {
		t.Run(proto, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					ts := httptest.NewServer(tc.handler)
					defer ts.Close()
					c := newHTTPCLient(http.DefaultTransport)
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					parse, err := url.Parse(ts.URL)
					require.NoError(t, err)
					_, err = c.get(ctx, parse)
					if tc.expectedError != "" {
						require.ErrorContains(t, err, tc.expectedError)
						return
					}
					require.NoError(t, err, "Wasm download got an unexpected error: %v", err)
				})
			}
		})
	}
}
