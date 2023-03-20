package wasm

import (
	"bytes"
	"context"
	_ "embed"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func Test_outputBinding_getMetadata(t *testing.T) {
	m := &outputBinding{}

	type testCase struct {
		name        string
		metadata    metadata.Base
		expected    *initMetadata
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
				"path": "./testdata",
			}},
			// Below ends in "is a directory" in unix, and "The handle is invalid." in windows.
			expectedErr: "error reading path: read ./testdata: ",
		},
	}

	for _, tt := range tests {
		tc := tt
		t.Run(tc.name, func(t *testing.T) {
			md, err := m.getInitMetadata(bindings.Metadata{Base: tc.metadata})
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

func Test_outputBinding_Init(t *testing.T) {
	type testCase struct {
		name        string
		metadata    metadata.Base
		expectedErr string
	}

	tests := []testCase{
		// This just tests the error message prefixes properly. Otherwise, it is
		// redundant to Test_outputBinding_getMetadata
		{
			name:        "requires path metadata",
			metadata:    metadata.Base{Properties: map[string]string{}},
			expectedErr: "wasm: failed to parse metadata: missing path",
		},
		// This is more than Test_outputBinding_getMetadata, as it ensures the
		// contents are actually wasm.
		{
			name: "path not wasm",
			metadata: metadata.Base{Properties: map[string]string{
				"path": "./testdata/main.go",
			}},
			expectedErr: "wasm: error compiling binary: invalid magic number",
		},
		{
			name: "ok",
			metadata: metadata.Base{Properties: map[string]string{
				"path": "./testdata/main.wasm",
			}},
		},
	}

	for _, tt := range tests {
		tc := tt
		t.Run(tc.name, func(t *testing.T) {
			m := NewWasmOutput(logger.NewLogger(t.Name())).(*outputBinding)
			defer m.Close()
			err := m.Init(context.Background(), bindings.Metadata{Base: tc.metadata})
			if tc.expectedErr == "" {
				require.NoError(t, err)
				require.NotNil(t, m.runtime)
				require.NotNil(t, m.module)
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
		// main.wasm was compiled via the following:
		//	tinygo build -o main.wasm -scheduler=none --no-debug -target=wasm main.go`
		"path": "./testdata/main.wasm",
	}}

	output := NewWasmOutput(l)
	defer output.(io.Closer).Close()

	err := output.Init(context.Background(), bindings.Metadata{Base: meta})
	require.NoError(t, err)

	ctx := context.Background()

	resp, err := output.Invoke(ctx, &bindings.InvokeRequest{
		Metadata:  map[string]string{"args": "salaboy"},
		Operation: bindings.GetOperation,
	})
	require.NoError(t, err)
	require.Equal(t, "hello salaboy", string(resp.Data))
}
