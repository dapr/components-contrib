package wasm

import (
	"bytes"
	"context"
	_ "embed"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"

	"github.com/dapr/components-contrib/metadata"
)

const (
	urlArgsFile  = "file://testdata/args/main.wasm"
	urlPythonOCI = "oci://ghcr.io/vmware-labs/python-wasm:3.11.3"
)

//go:embed testdata/args/main.wasm
var binArgs []byte

func TestGetInitMetadata(t *testing.T) {
	type testCase struct {
		name        string
		metadata    metadata.Base
		expected    *InitMetadata
		expectedErr string
	}

	tests := []testCase{
		{
			name: "file valid",
			metadata: metadata.Base{Properties: map[string]string{
				"url": urlArgsFile,
			}},
			expected: &InitMetadata{
				URL:       urlArgsFile,
				Guest:     binArgs,
				GuestName: "main",
			},
		},
		{
			name: "file valid - strictSandbox",
			metadata: metadata.Base{Properties: map[string]string{
				"url":           urlArgsFile,
				"strictSandbox": "true",
			}},
			expected: &InitMetadata{
				URL:           urlArgsFile,
				Guest:         binArgs,
				GuestName:     "main",
				StrictSandbox: true,
			},
		},
		{
			name:        "empty url",
			metadata:    metadata.Base{Properties: map[string]string{}},
			expectedErr: "missing url",
		},
		{
			name: "http invalid",
			metadata: metadata.Base{Properties: map[string]string{
				"url": "http:// ",
			}},
			expectedErr: "parse \"http:// \": invalid character \" \" in host name",
		},
		{
			name: "https invalid",
			metadata: metadata.Base{Properties: map[string]string{
				"url": "https:// ",
			}},
			expectedErr: "parse \"https:// \": invalid character \" \" in host name",
		},
		{
			name: "TODO oci",
			metadata: metadata.Base{Properties: map[string]string{
				"url": urlPythonOCI,
			}},
			expectedErr: "TODO oci",
		},
		{
			name: "TODO http",
			metadata: metadata.Base{Properties: map[string]string{
				"url": "http://foo/bar.wasm",
			}},
			expectedErr: "TODO http",
		},
		{
			name: "TODO https",
			metadata: metadata.Base{Properties: map[string]string{
				"url": "https://foo/bar.wasm",
			}},
			expectedErr: "TODO https",
		},
		{
			name: "unsupported scheme",
			metadata: metadata.Base{Properties: map[string]string{
				"url": "ldap://foo/bar.wasm",
			}},
			expectedErr: "unsupported URL scheme: ldap",
		},
		{
			name: "file not found",
			metadata: metadata.Base{Properties: map[string]string{
				"url": "file://testduta",
			}},
			expectedErr: "open testduta: ",
		},
		{
			name: "file dir not file",
			metadata: metadata.Base{Properties: map[string]string{
				"url": "file://testdata",
			}},
			// Below ends in "is a directory" in unix, and "The handle is invalid." in windows.
			expectedErr: "read testdata: ",
		},
	}

	for _, tt := range tests {
		tc := tt
		t.Run(tc.name, func(t *testing.T) {
			md, err := GetInitMetadata(tc.metadata)
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

//go:embed testdata/strict/main.wasm
var binStrict []byte

func TestNewModuleConfig(t *testing.T) {
	type testCase struct {
		name                     string
		metadata                 *InitMetadata
		minDuration, maxDuration time.Duration
	}

	tests := []testCase{
		{
			name:     "strictSandbox = false",
			metadata: &InitMetadata{Guest: binStrict},
			// In CI, Nanosleep(50ms) returned after 197ms.
			// As we can't control the platform clock, we have to be lenient
			minDuration: 50 * time.Millisecond,
			maxDuration: 50 * time.Millisecond * 5,
		},
		{
			name:        "strictSandbox = true",
			metadata:    &InitMetadata{StrictSandbox: true, Guest: binStrict},
			minDuration: 10 * time.Microsecond,
			maxDuration: 1 * time.Millisecond,
		},
	}

	ctx := context.Background()
	rt := wazero.NewRuntime(ctx)
	defer rt.Close(ctx)
	wasi_snapshot_preview1.MustInstantiate(ctx, rt)

	for _, tt := range tests {
		tc := tt
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer

			cfg := NewModuleConfig(tc.metadata).
				WithStdout(&out).WithStderr(&out).
				WithStartFunctions() // don't include instantiation in duration
			mod, err := rt.InstantiateWithConfig(ctx, tc.metadata.Guest, cfg)
			require.NoError(t, err)

			start := time.Now()
			_, err = mod.ExportedFunction("_start").Call(ctx)
			require.NoError(t, err)
			duration := time.Since(start)

			// TODO: TinyGo doesn't seem to use monotonic time. Track below:
			// https://github.com/tinygo-org/tinygo/issues/3776
			deterministicOut := `2000000
1000000
6393cff83a
`
			if tc.metadata.StrictSandbox {
				require.Equal(t, deterministicOut, out.String())
			} else {
				require.NotEqual(t, deterministicOut, out.String())
			}
			require.True(t, duration > tc.minDuration && duration < tc.maxDuration, duration)
		})
	}
}
