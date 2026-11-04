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
	"math"
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
	testCtx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

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
				"url": "http://foo.invalid/bar.wasm",
			}},
			expectedErr: "no such host",
		},
		{
			name: "TODO https",
			metadata: metadata.Base{Properties: map[string]string{
				"url": "https://foo.invalid/bar.wasm",
			}},
			expectedErr: "no such host",
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
			md, err := GetInitMetadata(testCtx, tc.metadata)
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
	// The guest (testdata/strict/main.go) reads the clock, sleeps 50ms, and prints random bytes.
	const guestSleep = 50 * time.Millisecond
	// In strict mode the clock and random source are fake, so the output is always the same.
	const deterministicOut = `2000000
1000000
3e0a4fc818
`
	// Strict mode is checked over several runs, because only the fastest one is compared to guestSleep.
	const strictRuns = 5

	ctx := t.Context()
	rt := wazero.NewRuntime(ctx)
	defer rt.Close(ctx)
	wasi_snapshot_preview1.MustInstantiate(ctx, rt)

	// run starts the guest once and returns its output and how long it ran on the host clock.
	run := func(t *testing.T, m *InitMetadata) (string, time.Duration) {
		t.Helper()
		var out bytes.Buffer
		cfg := NewModuleConfig(m).
			WithStdout(&out).WithStderr(&out).
			WithStartFunctions() // don't include instantiation in duration
		mod, err := rt.InstantiateWithConfig(ctx, m.Guest, cfg)
		require.NoError(t, err)
		defer mod.Close(ctx)

		start := time.Now()
		_, err = mod.ExportedFunction("_start").Call(ctx)
		// Context: https://github.com/tetratelabs/wazero/pull/2367
		require.NoError(t, err)
		return out.String(), time.Since(start)
	}

	t.Run("strictSandbox = false", func(t *testing.T) {
		out, duration := run(t, &InitMetadata{Guest: binStrict})

		// TODO: TinyGo doesn't seem to use monotonic time. Track below:
		// https://github.com/tinygo-org/tinygo/issues/3776
		require.NotEqual(t, deterministicOut, out)
		// A real sleep never returns early. There is no upper bound: how late it returns depends on the host.
		require.GreaterOrEqual(t, duration, guestSleep)
	})

	t.Run("strictSandbox = true", func(t *testing.T) {
		fastest := time.Duration(math.MaxInt64)
		for range strictRuns {
			out, duration := run(t, &InitMetadata{StrictSandbox: true, Guest: binStrict})
			require.Equal(t, deterministicOut, out)
			fastest = min(fastest, duration)
		}
		// Nanosleep returns immediately, so the guest must not really sleep. A real sleep takes at least guestSleep
		// on every run, while a slow host would have to stall on all of them to fail this.
		// There is no lower bound: the host clock's resolution on Windows is too coarse to measure a run this short.
		require.Less(t, fastest, guestSleep)
	})
}
