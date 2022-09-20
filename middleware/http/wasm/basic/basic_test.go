package basic

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/dapr/components-contrib/metadata"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/components-contrib/middleware/http/wasm/basic/internal/test"
	"github.com/dapr/kit/logger"
)

var exampleWasm []byte

// TestMain ensures we can read the example wasm prior to running unit tests.
func TestMain(m *testing.M) {
	var err error
	exampleWasm, err = os.ReadFile("example/example.wasm")
	if err != nil {
		log.Panicln(err)
	}
	os.Exit(m.Run())
}

func Test_NewMiddleWare(t *testing.T) {
	l := test.NewLogger()
	require.Equal(t, &wapcMiddleware{logger: l}, NewMiddleware(l))
}

func Test_wapcMiddleware_log(t *testing.T) {
	l := test.NewLogger()
	m := &wapcMiddleware{logger: l}
	message := "alert"
	m.log(message)

	require.Equal(t, "Info(alert)\n", l.(fmt.Stringer).String())
}

func Test_wapcMiddleware_getMetadata(t *testing.T) {
	m := &wapcMiddleware{}

	type testCase struct {
		name        string
		metadata    metadata.Base
		expected    *middlewareMetadata
		expectedErr string
	}

	tests := []testCase{
		{
			name:        "empty path",
			metadata:    metadata.Base{Properties: map[string]string{}},
			expectedErr: "missing path",
		},
		{
			name: "path dir not file",
			metadata: metadata.Base{Properties: map[string]string{
				"path": "./example",
			}},
			// Below ends in "is a directory" in unix, and "The handle is invalid." in windows.
			expectedErr: "error reading path: read ./example: ",
		},
		{
			name: "poolSize defaults to 10",
			metadata: metadata.Base{Properties: map[string]string{
				"path": "./example/example.wasm",
			}},
			expected: &middlewareMetadata{Path: "./example/example.wasm", PoolSize: 10, guest: exampleWasm},
		},
		{
			name: "poolSize",
			metadata: metadata.Base{Properties: map[string]string{
				"path":     "./example/example.wasm",
				"poolSize": "1",
			}},
			expected: &middlewareMetadata{Path: "./example/example.wasm", PoolSize: 1, guest: exampleWasm},
		},
		{
			name: "poolSize invalid",
			metadata: metadata.Base{Properties: map[string]string{
				"path":     "./example/example.wasm",
				"poolSize": "-1",
			}},
			expectedErr: `invalid poolSize: strconv.ParseUint: parsing "-1": invalid syntax`,
		},
	}

	for _, tt := range tests {
		tc := tt
		t.Run(tc.name, func(t *testing.T) {
			md, err := m.getMetadata(middleware.Metadata{Base: tc.metadata})
			if tc.expectedErr == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expected, md)
			} else {
				// Use substring match as the error can be different in Windows.
				require.Contains(t, err.Error(), tc.expectedErr)
			}
		})
	}
}

func Test_wapcMiddleware_getHandler(t *testing.T) {
	m := &wapcMiddleware{logger: logger.NewLogger(t.Name())}

	type testCase struct {
		name        string
		metadata    metadata.Base
		expectedErr string
	}

	tests := []testCase{
		// This just tests the error message prefixes properly. Otherwise, it is
		// redundant to Test_wapcMiddleware_getMetadata
		{
			name:        "requires path metadata",
			metadata:    metadata.Base{Properties: map[string]string{}},
			expectedErr: "wasm basic: failed to parse metadata: missing path",
		},
		// This is more than Test_wapcMiddleware_getMetadata, as it ensures the
		// contents are actually wasm.
		{
			name: "path not wasm",
			metadata: metadata.Base{Properties: map[string]string{
				"path": "./example/example.go",
			}},
			expectedErr: "wasm basic: error compiling wasm at ./example/example.go: invalid binary",
		},
		{
			name: "ok",
			metadata: metadata.Base{Properties: map[string]string{
				"path": "./example/example.wasm",
			}},
		},
	}

	for _, tt := range tests {
		tc := tt
		t.Run(tc.name, func(t *testing.T) {
			h, err := m.getHandler(middleware.Metadata{Base: tc.metadata})
			if tc.expectedErr == "" {
				require.NoError(t, err)
				require.NotNil(t, h.mod)
				require.NotNil(t, h.pool)
			} else {
				require.EqualError(t, err, tc.expectedErr)
			}
		})
	}
}

func Test_Example(t *testing.T) {
	meta := metadata.Base{Properties: map[string]string{
		// example.wasm was compiled via the following:
		//	tinygo build -o example.wasm -scheduler=none --no-debug -target=wasi hello.go`
		"path":     "./example/example.wasm",
		"poolSize": "2",
	}}
	l := test.NewLogger()
	handlerFn, err := NewMiddleware(l).GetHandler(middleware.Metadata{Base: meta})
	require.NoError(t, err)
	handler := handlerFn(func(*fasthttp.RequestCtx) {})

	var ctx fasthttp.RequestCtx
	ctx.Request.SetRequestURI("/v1.0/hi")
	handler(&ctx)
	require.Equal(t, "/v1.0/hello", string(ctx.RequestURI()))
	require.Empty(t, l.(fmt.Stringer).String())
}
