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

package cloudmap

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/servicediscovery"
	"github.com/aws/aws-sdk-go-v2/service/servicediscovery/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
)

type mockServiceDiscoveryClient struct {
	namespaces []types.NamespaceSummary
	instances  []types.HttpInstanceSummary
}

func (m *mockServiceDiscoveryClient) GetNamespace(ctx context.Context, input *servicediscovery.GetNamespaceInput, opts ...func(*servicediscovery.Options)) (*servicediscovery.GetNamespaceOutput, error) {
	return &servicediscovery.GetNamespaceOutput{
		Namespace: &types.Namespace{
			Id:   aws.String("ns-12345"),
			Name: aws.String("test-namespace"),
		},
	}, nil
}

func (m *mockServiceDiscoveryClient) ListNamespaces(ctx context.Context, input *servicediscovery.ListNamespacesInput, opts ...func(*servicediscovery.Options)) (*servicediscovery.ListNamespacesOutput, error) {
	return &servicediscovery.ListNamespacesOutput{
		Namespaces: m.namespaces,
	}, nil
}

func (m *mockServiceDiscoveryClient) DiscoverInstances(ctx context.Context, input *servicediscovery.DiscoverInstancesInput, opts ...func(*servicediscovery.Options)) (*servicediscovery.DiscoverInstancesOutput, error) {
	return &servicediscovery.DiscoverInstancesOutput{
		Instances: m.instances,
	}, nil
}

func TestNewResolver(t *testing.T) {
	resolver := NewResolver(logger.NewLogger("test"))
	assert.NotNil(t, resolver)
}

func TestValidateMetadata(t *testing.T) {
	tests := []struct {
		name          string
		metadata      cloudMapMetadata
		expectedError bool
	}{
		{
			name: "valid with namespace name",
			metadata: cloudMapMetadata{
				NamespaceName: "test-namespace",
			},
			expectedError: false,
		},
		{
			name: "valid with namespace ID",
			metadata: cloudMapMetadata{
				NamespaceID: "ns-12345",
			},
			expectedError: false,
		},
		{
			name:          "invalid - no namespace",
			metadata:      cloudMapMetadata{},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.metadata.Validate()
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestResolveWithMockClient(t *testing.T) {
	resolver := &Resolver{
		logger:          logger.NewLogger("test"),
		namespaceName:   "test-namespace",
		defaultDaprPort: 3500,
	}

	mockClient := &mockServiceDiscoveryClient{
		instances: []types.HttpInstanceSummary{
			{
				InstanceId: aws.String("instance-1"),
				Attributes: map[string]string{
					"AWS_INSTANCE_IPV4": "10.0.0.1",
					"DAPR_PORT":         "50001",
				},
			},
			{
				InstanceId: aws.String("instance-2"),
				Attributes: map[string]string{
					"AWS_INSTANCE_IPV4": "10.0.0.2",
					"DAPR_PORT":         "50002",
				},
			},
		},
	}
	resolver.client = mockClient

	request := nameresolution.ResolveRequest{
		ID:        "test-service",
		Namespace: "test-namespace",
		Port:      0, // Use port from instance attributes
	}

	// Test ResolveID
	address, err := resolver.ResolveID(context.Background(), request)
	require.NoError(t, err)
	assert.Contains(t, []string{"10.0.0.1:50001", "10.0.0.2:50002"}, address)

	// Test ResolveIDMulti
	addresses, err := resolver.ResolveIDMulti(context.Background(), request)
	require.NoError(t, err)
	assert.Len(t, addresses, 2)
	assert.Contains(t, addresses, "10.0.0.1:50001")
	assert.Contains(t, addresses, "10.0.0.2:50002")
}

func TestExtractInstanceAddress(t *testing.T) {
	resolver := &Resolver{
		defaultDaprPort: 3500,
	}

	tests := []struct {
		name         string
		instance     types.HttpInstanceSummary
		requestedPort int
		expected     string
		expectError  bool
	}{
		{
			name: "IPv4 with custom port",
			instance: types.HttpInstanceSummary{
				Attributes: map[string]string{
					"AWS_INSTANCE_IPV4": "10.0.0.1",
					"DAPR_PORT":         "50001",
				},
			},
			requestedPort: 0,
			expected:     "10.0.0.1:50001",
		},
		{
			name: "IPv4 with requested port override",
			instance: types.HttpInstanceSummary{
				Attributes: map[string]string{
					"AWS_INSTANCE_IPV4": "10.0.0.1",
					"DAPR_PORT":         "50001",
				},
			},
			requestedPort: 8080,
			expected:     "10.0.0.1:8080",
		},
		{
			name: "IPv4 with default port",
			instance: types.HttpInstanceSummary{
				Attributes: map[string]string{
					"AWS_INSTANCE_IPV4": "10.0.0.1",
				},
			},
			requestedPort: 0,
			expected:     "10.0.0.1:3500",
		},
		{
			name: "IPv6 address",
			instance: types.HttpInstanceSummary{
				Attributes: map[string]string{
					"AWS_INSTANCE_IPV6": "2001:db8::1",
					"DAPR_PORT":         "50001",
				},
			},
			requestedPort: 0,
			expected:     "2001:db8::1:50001",
		},
		{
			name: "CNAME address",
			instance: types.HttpInstanceSummary{
				Attributes: map[string]string{
					"AWS_INSTANCE_CNAME": "service.example.com",
					"DAPR_PORT":          "50001",
				},
			},
			requestedPort: 0,
			expected:     "service.example.com:50001",
		},
		{
			name: "no address attributes",
			instance: types.HttpInstanceSummary{
				Attributes: map[string]string{
					"DAPR_PORT": "50001",
				},
			},
			requestedPort: 0,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			address, err := resolver.extractInstanceAddress(tt.instance, tt.requestedPort)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, address)
			}
		})
	}
}