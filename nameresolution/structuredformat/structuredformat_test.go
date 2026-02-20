/*
Copyright 2025 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package structuredformat

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	nr "github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
)

const (
	validJSON = `{
		"appInstances": {
			"myapp": [
				{
					"domain": "github.com",
					"ipv4": "",
					"ipv6": "",
					"port": 443
				}
			]
		}
	}`

	validJSONIPv6 = `{
		"appInstances": {
			"myapp": [
				{
					"domain": "",
					"ipv4": "",
					"ipv6": "::1",
					"port": 443
				}
			]
		}
	}`

	validYAML = `appInstances:
  myapp:
    - domain: ''
      ipv4: '127.127.127.127'
      ipv6: ''
      port: 443`

	invalidJSONNoHost = `{
		"appInstances": {
			"badapp": [
				{
					"domain": "",
					"ipv4": "",
					"ipv6": "",
					"port": 80
				}
			]
		}
	}`

	invalidJSONBadPort = `{
		"appInstances": {
			"badapp": [
				{
					"domain": "example.com",
					"port": -1
				}
			]
		}
	}`
)

// Helper to create temp file for file-based tests
func writeTempFile(t *testing.T, content string, ext string) string {
	t.Helper()
	tmpFile := filepath.Join(t.TempDir(), "config"+ext)
	err := os.WriteFile(tmpFile, []byte(content), 0o600)
	require.NoError(t, err)
	return tmpFile
}

func TestInit(t *testing.T) {
	tests := []struct {
		name          string
		metadata      nr.Metadata
		expectedError string
	}{
		{
			name: "valid json string",
			metadata: nr.Metadata{
				Configuration: map[string]string{
					"structuredType": "json",
					"stringValue":    validJSON,
				},
			},
		},
		{
			name: "valid yaml string",
			metadata: nr.Metadata{
				Configuration: map[string]string{
					"structuredType": "yaml",
					"stringValue":    validYAML,
				},
			},
		},
		{
			name: "valid json file",
			metadata: nr.Metadata{
				Configuration: map[string]string{
					"structuredType": "jsonFile",
					"filePath":       writeTempFile(t, validJSON, ".json"),
				},
			},
		},
		{
			name: "valid yaml file",
			metadata: nr.Metadata{
				Configuration: map[string]string{
					"structuredType": "yamlFile",
					"filePath":       writeTempFile(t, validYAML, ".yaml"),
				},
			},
		},
		{
			name: "missing stringValue for json",
			metadata: nr.Metadata{
				Configuration: map[string]string{
					"structuredType": "json",
				},
			},
			expectedError: "stringValue is required when structuredType is \"json\"",
		},
		{
			name: "missing filePath for jsonFile",
			metadata: nr.Metadata{
				Configuration: map[string]string{
					"structuredType": "jsonFile",
				},
			},
			expectedError: "filePath is required when structuredType is \"jsonFile\"",
		},
		{
			name: "invalid structuredType",
			metadata: nr.Metadata{
				Configuration: map[string]string{
					"structuredType": "invalid",
					"stringValue":    validJSON,
				},
			},
			expectedError: "invalid structuredType \"invalid\"; must be one of: json, yaml, jsonFile, yamlFile",
		},
		{
			name: "invalid address: no host",
			metadata: nr.Metadata{
				Configuration: map[string]string{
					"structuredType": "json",
					"stringValue":    invalidJSONNoHost,
				},
			},
			expectedError: "invalid address at AppInstances[\"badapp\"][0]: missing domain, ipv4, and ipv6",
		},
		{
			name: "invalid address: bad port",
			metadata: nr.Metadata{
				Configuration: map[string]string{
					"structuredType": "json",
					"stringValue":    invalidJSONBadPort,
				},
			},
			expectedError: "invalid port -1 for AppInstances[\"badapp\"][0]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewResolver(logger.NewLogger("test"))
			err := r.Init(t.Context(), tt.metadata)

			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestResolveID(t *testing.T) {
	tests := []struct {
		name           string
		structuredType string
		stringValue    string
		request        nr.ResolveRequest
		expectedResult string
		expectedError  string
	}{
		{
			name:           "resolve by domain",
			structuredType: "json",
			stringValue:    validJSON,
			request:        nr.ResolveRequest{ID: "myapp"},
			expectedResult: "github.com:443",
		},
		{
			name:           "resolve by IPv6",
			structuredType: "json",
			stringValue:    validJSONIPv6,
			request:        nr.ResolveRequest{ID: "myapp"},
			expectedResult: "[::1]:443",
		},
		{
			name:           "resolve by IPv4 from YAML",
			structuredType: "yaml",
			stringValue:    validYAML,
			request:        nr.ResolveRequest{ID: "myapp"},
			expectedResult: "127.127.127.127:443",
		},
		{
			name:           "non-existent app ID",
			structuredType: "json",
			stringValue:    validJSON,
			request:        nr.ResolveRequest{ID: "unknown"},
			expectedError:  "no services found with ID \"unknown\"",
		},
		{
			name:           "empty app ID",
			structuredType: "json",
			stringValue:    validJSON,
			request:        nr.ResolveRequest{ID: ""},
			expectedError:  "empty ID not allowed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewResolver(logger.NewLogger("test"))
			err := r.Init(t.Context(), nr.Metadata{
				Configuration: map[string]string{
					"structuredType": tt.structuredType,
					"stringValue":    tt.stringValue,
				},
			})
			require.NoError(t, err)

			result, err := r.ResolveID(t.Context(), tt.request)

			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedResult, result)
			}
		})
	}
}

func TestClose(t *testing.T) {
	r := NewResolver(logger.NewLogger("test"))
	err := r.Close()
	require.NoError(t, err)
}
