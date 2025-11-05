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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	nr "github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
)

var (
	jsonValue = `{
    "appInstances": {
        "myapp": [
            {
                "domain": "github.com",
                "ipv4": "",
                "ipv6": "",
                "port": 443,
                "extendedInfo": {
                    "hello": "world"
                }
            }
        ]
    }
}`

	jsonValueIPV6 = `{
		"appInstances": {
			"myapp": [
				{
					"domain": "",
					"ipv4": "",
					"ipv6": "::1",
					"port": 443,
					"extendedInfo": {
						"hello": "world"
					}
				}
			]
		}
	}`

	yamlValue = `appInstances:
  myapp:
    - domain: ''
      ipv4: '127.127.127.127'
      ipv6: ''
      port: 443
      extendedInfo:
        hello: world`
)

func TestInit(t *testing.T) {
	tests := []struct {
		name          string
		metadata      nr.Metadata
		expectedError string
	}{
		{
			name: "valid metadata with json string format",
			metadata: nr.Metadata{
				Configuration: map[string]string{
					"structuredType": "jsonString",
					"stringValue":    jsonValue,
				},
			},
		},
		{
			name: "valid metadata with json string format ipv6",
			metadata: nr.Metadata{
				Configuration: map[string]string{
					"structuredType": "jsonString",
					"stringValue":    jsonValueIPV6,
				},
			},
		},
		{
			name: "valid metadata with yaml string format",
			metadata: nr.Metadata{
				Configuration: map[string]string{
					"structuredType": "yamlString",
					"stringValue":    yamlValue,
				},
			},
		},
		{
			name: "invalid structuredType",
			metadata: nr.Metadata{
				Configuration: map[string]string{
					"structuredType": "invalidType",
					"stringValue":    yamlValue,
				},
			},
			expectedError: "structuredType must be one of: jsonString, yamlString, jsonFile, yamlFile",
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
			name:           "valid app name with json string format",
			structuredType: "jsonString",
			stringValue:    jsonValue,
			request: nr.ResolveRequest{
				ID: "myapp",
			},
			expectedResult: "github.com:443",
		},
		{
			name:           "valid app name with json string format ipv6",
			structuredType: "jsonString",
			stringValue:    jsonValueIPV6,
			request: nr.ResolveRequest{
				ID: "myapp",
			},
			expectedResult: "[::1]:443",
		},
		{
			name:           "valid app name with yaml string format",
			structuredType: "yamlString",
			stringValue:    yamlValue,
			request: nr.ResolveRequest{
				ID: "myapp",
			},
			expectedResult: "127.127.127.127:443",
		},
		{
			name:           "Verify non-existent app_id",
			structuredType: "yamlString",
			stringValue:    yamlValue,
			request: nr.ResolveRequest{
				ID: "non-existentAppID",
			},
			expectedError: "no services found with AppID 'non-existentAppID'",
		},
		{
			name:           "empty app name",
			structuredType: "yamlString",
			stringValue:    yamlValue,
			request: nr.ResolveRequest{
				ID: "",
			},
			expectedError: "empty ID not allowed",
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
