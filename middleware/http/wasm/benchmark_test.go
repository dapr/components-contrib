package wasm

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"

	dapr "github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/internal/httputils"
	"github.com/dapr/components-contrib/metadata"
)

const parallel = 10

func BenchmarkNative(b *testing.B) {
	benchmarkAll(b, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if httputils.RequestURI(r) == "/v1.0/hi?name=panda" {
				httputils.SetRequestURI(r, "/v1.0/hello?name=teddy")
			}
			next.ServeHTTP(w, r)
		})
	})
}

func BenchmarkTinygo(b *testing.B) {
	path := "./internal/e2e-guests/rewrite/main.wasm"
	benchmarkMiddleware(b, path)
}

// BenchmarkWat gives baseline performance for the same handler by
// writing it directly in WebAssembly Text Format.
func BenchmarkWat(b *testing.B) {
	path := "./internal/testdata/rewrite.wasm"
	benchmarkMiddleware(b, path)
}

func benchmarkMiddleware(b *testing.B, path string) {
	md := metadata.Base{Properties: map[string]string{"path": path}}

	l := logger.NewLogger(b.Name())
	l.SetOutput(io.Discard)

	handlerFn, err := NewMiddleware(l).GetHandler(dapr.Metadata{Base: md})
	if err != nil {
		b.Fatal(err)
	}
	benchmarkAll(b, handlerFn)
}

var benches = map[string]struct {
	newRequest func() *http.Request
	test       http.Handler
}{
	"rewrite": {
		newRequest: func() *http.Request {
			u, err := url.Parse("https://test.io/v1.0/hi?name=panda")
			if err != nil {
				panic(err)
			}
			return &http.Request{
				Method: http.MethodGet,
				URL:    u,
				Host:   "test.io",
				Header: map[string][]string{
					"User-Agent": {"curl/7.79.1"},
					"Accept":     {"*/*"},
				},
			}
		},
		test: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if method := r.Method; method != http.MethodGet {
				body := fmt.Sprintf("Unexpected request method: %q", method)
				httputils.RespondWithErrorAndMessage(w, http.StatusInternalServerError, body)
			}
			if path := r.URL.Path; path != "/v1.0/hello" {
				body := fmt.Sprintf("Expected wasm to rewrite path: %s", path)
				httputils.RespondWithErrorAndMessage(w, http.StatusInternalServerError, body)
			}
			if query := r.URL.RawQuery; query != "name=teddy" {
				body := fmt.Sprintf("Expected wasm to retain query: %s", query)
				httputils.RespondWithErrorAndMessage(w, http.StatusInternalServerError, body)
			}
			w.Header().Set("Content-Type", "text/plain")
			if _, err := w.Write([]byte("Hello, world!")); err != nil {
				panic(err)
			}
		}),
	},
}

func benchmarkAll(b *testing.B, mw func(http.Handler) http.Handler) {
	for n, s := range benches {
		b.Run(n, func(b *testing.B) {
			b.SetParallelism(parallel)
			benchmark(b, mw, n, s.newRequest, s.test)
		})
	}
}

func benchmark(
	b *testing.B,
	mw func(http.Handler) http.Handler,
	name string,
	newRequest func() *http.Request,
	test http.Handler,
) {
	h := mw(test)
	b.Run(name, func(b *testing.B) {
		// We don't report allocations because memory allocations for TinyGo are
		// in wasm which isn't visible to the Go benchmark.
		for i := 0; i < b.N; i++ {
			h.ServeHTTP(fakeResponseWriter{}, newRequest())
		}
	})
}

var _ http.ResponseWriter = fakeResponseWriter{}

type fakeResponseWriter struct{}

func (rw fakeResponseWriter) Header() http.Header {
	return http.Header{}
}

func (rw fakeResponseWriter) Write(b []byte) (int, error) {
	return len(b), nil
}

func (rw fakeResponseWriter) WriteHeader(statusCode int) {
	// None of our benchmark tests should send failure status. If there's a
	// failure, it is likely there's a problem in the test data.
	if statusCode != 200 {
		panic(statusCode)
	}
}
