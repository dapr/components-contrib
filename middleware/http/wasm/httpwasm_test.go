package wasm

import (
	"bytes"
	_ "embed"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dapr/components-contrib/internal/httputils"

	"github.com/dapr/components-contrib/metadata"

	"github.com/http-wasm/http-wasm-host-go/api"

	"github.com/stretchr/testify/require"

	dapr "github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

func Test_NewMiddleWare(t *testing.T) {
	l := logger.NewLogger(t.Name())
	require.Equal(t, &middleware{logger: l}, NewMiddleware(l))
}

func Test_middleware_log(t *testing.T) {
	l := logger.NewLogger(t.Name())
	var buf bytes.Buffer
	l.SetOutput(&buf)

	m := &middleware{logger: l}
	message := "alert"
	m.Log(ctx, api.LogLevelInfo, message)

	require.Contains(t, buf.String(), `level=info msg=alert`)
}

func Test_middleware_getMetadata(t *testing.T) {
	m := &middleware{}

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
	}

	for _, tt := range tests {
		tc := tt
		t.Run(tc.name, func(t *testing.T) {
			md, err := m.getMetadata(dapr.Metadata{Base: tc.metadata})
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

func Test_middleware_getHandler(t *testing.T) {
	m := &middleware{logger: logger.NewLogger(t.Name())}

	type testCase struct {
		name        string
		metadata    metadata.Base
		expectedErr string
	}

	tests := []testCase{
		// This just tests the error message prefixes properly. Otherwise, it is
		// redundant to Test_middleware_getMetadata
		{
			name:        "requires path metadata",
			metadata:    metadata.Base{Properties: map[string]string{}},
			expectedErr: "wasm basic: failed to parse metadata: missing path",
		},
		// This is more than Test_middleware_getMetadata, as it ensures the
		// contents are actually wasm.
		{
			name: "path not wasm",
			metadata: metadata.Base{Properties: map[string]string{
				"path": "./example/router.go",
			}},
			expectedErr: "wasm: error compiling guest: invalid binary",
		},
		{
			name: "ok",
			metadata: metadata.Base{Properties: map[string]string{
				"path": "./example/router.wasm",
			}},
		},
	}

	for _, tt := range tests {
		tc := tt
		t.Run(tc.name, func(t *testing.T) {
			h, err := m.getHandler(dapr.Metadata{Base: tc.metadata})
			if tc.expectedErr == "" {
				require.NoError(t, err)
				require.NotNil(t, h.mw)
			} else {
				require.EqualError(t, err, tc.expectedErr)
			}
		})
	}
}

func Test_Example(t *testing.T) {
	l := logger.NewLogger(t.Name())
	var buf bytes.Buffer
	l.SetOutput(&buf)

	meta := metadata.Base{Properties: map[string]string{
		// router.wasm was compiled via the following:
		//	tinygo build -o router.wasm -scheduler=none --no-debug -target=wasi router.go`
		"path": "./example/router.wasm",
	}}
	handlerFn, err := NewMiddleware(l).GetHandler(dapr.Metadata{Base: meta})
	require.NoError(t, err)

	handler := handlerFn(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	r := httptest.NewRequest(http.MethodGet, "/host/hi?name=panda", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	require.Equal(t, "/hi?name=panda", httputils.RequestURI(r))
	require.Empty(t, buf.String())
}
