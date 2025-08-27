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

package nameformat

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	nr "github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
)

func TestInit(t *testing.T) {
	tests := []struct {
		name          string
		metadata      nr.Metadata
		expectedError string
	}{
		{
			name: "valid metadata with format",
			metadata: nr.Metadata{
				Configuration: map[string]string{
					"format": "service-{appid}.default.svc.cluster.local",
				},
			},
		},
		{
			name: "valid metadata with multiple placeholders",
			metadata: nr.Metadata{
				Configuration: map[string]string{
					"format": "{appid}-service-{appid}.example.com",
				},
			},
		},
		{
			name: "missing format",
			metadata: nr.Metadata{
				Configuration: map[string]string{},
			},
			expectedError: "format is required in metadata",
		},
		{
			name: "invalid format without placeholder",
			metadata: nr.Metadata{
				Configuration: map[string]string{
					"format": "service.example.com",
				},
			},
			expectedError: "must contain {appid} placeholder",
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
		format         string
		request        nr.ResolveRequest
		expectedResult string
		expectedError  string
	}{
		{
			name:   "valid app name with single placeholder",
			format: "service-{appid}.default.svc.cluster.local",
			request: nr.ResolveRequest{
				ID: "myapp",
			},
			expectedResult: "service-myapp.default.svc.cluster.local",
		},
		{
			name:   "valid app name with multiple placeholders",
			format: "{appid}-service-{appid}.example.com",
			request: nr.ResolveRequest{
				ID: "frontend",
			},
			expectedResult: "frontend-service-frontend.example.com",
		},
		{
			name:   "empty app name",
			format: "service-{appid}.example.com",
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
					"format": tt.format,
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
