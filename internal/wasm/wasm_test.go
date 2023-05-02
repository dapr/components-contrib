package wasm

import (
	"context"
	_ "embed"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
)

const (
	urlArgsFile  = "file://testdata/args/main.wasm"
	urlPythonOCI = "oci://ghcr.io/vmware-labs/python-wasm:3.11.3"
)

//go:embed testdata/args/main.wasm
var urlArgsBin []byte

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
				Guest:     urlArgsBin,
				GuestName: "main",
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
			ctx := context.Background()
			md, err := GetInitMetadata(ctx, tc.metadata)
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
