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
	wasmBinary := append(wasmMagicNumber, 0x00, 0x00, 0x00, 0x00)
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
					fetcher := newHTTPFetcher(http.DefaultTransport)
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					parse, err := url.Parse(ts.URL)
					require.NoError(t, err)
					_, err = fetcher.fetch(ctx, parse)
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
