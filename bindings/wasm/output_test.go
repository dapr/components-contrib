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

const (
	urlArgsFile    = "file://testdata/args/main.wasm"
	urlExampleFile = "file://testdata/example/main.wasm"
	urlLoopFile    = "file://testdata/loop/main.wasm"
)

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
			name:        "requires url metadata",
			metadata:    metadata.Base{Properties: map[string]string{}},
			expectedErr: "wasm: failed to parse metadata: missing url",
		},
		// This is more than Test_outputBinding_getMetadata, as it ensures the
		// contents are actually wasm.
		{
			name: "url not wasm",
			metadata: metadata.Base{Properties: map[string]string{
				"url": "file://testdata/args/main.go",
			}},
			expectedErr: "wasm: error compiling binary: invalid magic number",
		},
		{
			name: "ok",
			metadata: metadata.Base{Properties: map[string]string{
				"url": urlArgsFile,
			}},
		},
	}

	for _, tt := range tests {
		tc := tt
		t.Run(tc.name, func(t *testing.T) {
			output := NewWasmOutput(logger.NewLogger(t.Name())).(*outputBinding)
			defer output.Close()

			err := output.Init(context.Background(), bindings.Metadata{Base: tc.metadata})
			if tc.expectedErr == "" {
				require.NoError(t, err)
				require.NotNil(t, output.runtime)
				require.NotNil(t, output.module)
			} else {
				require.EqualError(t, err, tc.expectedErr)
			}
		})
	}
}

func Test_Invoke(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	type testCase struct {
		name         string
		url          string
		request      *bindings.InvokeRequest
		ctx          context.Context
		expectedData string
		expectedErr  string
	}

	tests := []testCase{
		{
			name: "args none",
			url:  urlArgsFile,
			request: &bindings.InvokeRequest{
				Operation: ExecuteOperation,
			},
			expectedData: "main",
		},
		{
			name: "args empty",
			url:  urlArgsFile,
			request: &bindings.InvokeRequest{
				Metadata:  map[string]string{"args": ""},
				Operation: ExecuteOperation,
			},
			expectedData: "main",
		},
		{
			name: "args",
			url:  urlArgsFile,
			request: &bindings.InvokeRequest{
				Metadata:  map[string]string{"args": "1,2"},
				Operation: ExecuteOperation,
			},
			expectedData: "main\n1\n2",
		},
		{
			name: "canceled",
			url:  urlLoopFile,
			ctx:  canceledCtx,
			request: &bindings.InvokeRequest{
				Operation: ExecuteOperation,
			},
			expectedErr: `module closed with context canceled`,
		},
		{
			name: "example",
			url:  urlExampleFile,
			request: &bindings.InvokeRequest{
				Metadata:  map[string]string{"args": "salaboy"},
				Operation: ExecuteOperation,
			},
			expectedData: "hello salaboy",
		},
	}

	for _, tt := range tests {
		tc := tt
		t.Run(tc.name, func(t *testing.T) {
			l := logger.NewLogger(t.Name())
			var buf bytes.Buffer
			l.SetOutput(&buf)

			meta := metadata.Base{Properties: map[string]string{"url": tc.url}}

			output := NewWasmOutput(l)
			defer output.(io.Closer).Close()

			ctx := context.Background()

			err := output.Init(ctx, bindings.Metadata{Base: meta})
			require.NoError(t, err)

			reqCtx := tc.ctx
			if reqCtx == nil {
				reqCtx = ctx
			}

			if tc.expectedErr == "" {
				// execute twice to prove idempotency
				for i := 0; i < 2; i++ {
					resp, outputErr := output.Invoke(reqCtx, tc.request)
					require.NoError(t, outputErr)
					require.Equal(t, tc.expectedData, string(resp.Data))
				}
			} else {
				_, err = output.Invoke(reqCtx, tc.request)
				require.EqualError(t, err, tc.expectedErr)
			}
		})
	}
}
