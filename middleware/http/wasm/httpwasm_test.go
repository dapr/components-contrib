package wasm

import (
	"bytes"
	"context"
	_ "embed"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/http-wasm/http-wasm-host-go/api"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/internal/httputils"
	"github.com/dapr/components-contrib/metadata"
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
	m.Log(context.Background(), api.LogLevelInfo, message)

	require.Contains(t, buf.String(), `level=info msg=alert`)
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
			name:        "requires url metadata",
			metadata:    metadata.Base{Properties: map[string]string{}},
			expectedErr: "wasm: failed to parse metadata: missing url",
		},
		// This is more than Test_middleware_getMetadata, as it ensures the
		// contents are actually wasm.
		{
			name: "url not wasm",
			metadata: metadata.Base{Properties: map[string]string{
				"url": "file://example/router.go",
			}},
			expectedErr: "wasm: error compiling guest: invalid magic number",
		},
		{
			name: "ok",
			metadata: metadata.Base{Properties: map[string]string{
				"url": "file://example/router.wasm",
			}},
		},
	}

	for _, tt := range tests {
		tc := tt
		t.Run(tc.name, func(t *testing.T) {
			h, err := m.getHandler(context.Background(), dapr.Metadata{Base: tc.metadata})
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
		"url": "file://example/router.wasm",
	}}
	handlerFn, err := NewMiddleware(l).GetHandler(context.Background(), dapr.Metadata{Base: meta})
	require.NoError(t, err)

	handler := handlerFn(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	r := httptest.NewRequest(http.MethodGet, "/host/hi?name=panda", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	require.Equal(t, "/hi?name=panda", httputils.RequestURI(r))
	require.Empty(t, buf.String())
}

func Test_ioCloser(t *testing.T) {
	var _ io.Closer = &requestHandler{}
}
