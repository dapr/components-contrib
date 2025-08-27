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
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/servicediscovery"
	"github.com/aws/aws-sdk-go-v2/service/servicediscovery/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	awsAuth "github.com/dapr/components-contrib/common/authentication/aws"
	"github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
)

type mockServiceDiscoveryAPI struct {
	getNamespaceResp      *servicediscovery.GetNamespaceOutput
	getNamespaceErr       error
	listNamespacesResp    *servicediscovery.ListNamespacesOutput
	listNamespacesErr     error
	discoverInstancesResp *servicediscovery.DiscoverInstancesOutput
	discoverInstancesErr  error
}

func (m *mockServiceDiscoveryAPI) GetNamespace(ctx context.Context, input *servicediscovery.GetNamespaceInput, opts ...func(*servicediscovery.Options)) (*servicediscovery.GetNamespaceOutput, error) {
	return m.getNamespaceResp, m.getNamespaceErr
}

func (m *mockServiceDiscoveryAPI) ListNamespaces(ctx context.Context, input *servicediscovery.ListNamespacesInput, opts ...func(*servicediscovery.Options)) (*servicediscovery.ListNamespacesOutput, error) {
	return m.listNamespacesResp, m.listNamespacesErr
}

func (m *mockServiceDiscoveryAPI) DiscoverInstances(ctx context.Context, input *servicediscovery.DiscoverInstancesInput, opts ...func(*servicediscovery.Options)) (*servicediscovery.DiscoverInstancesOutput, error) {
	return m.discoverInstancesResp, m.discoverInstancesErr
}

type mockAuthProvider struct {
	awsAuth.Provider
	closeCalled bool
}

func (m *mockAuthProvider) Close() error {
	m.closeCalled = true
	return nil
}

func TestCloudMapResolver(t *testing.T) {
	t.Run("init with valid namespace ID", func(t *testing.T) {
		r := NewResolver(logger.NewLogger("test")).(*Resolver)
		mockClient := &mockServiceDiscoveryAPI{
			getNamespaceResp: &servicediscovery.GetNamespaceOutput{
				Namespace: &types.Namespace{
					Name: aws.String("test-namespace"),
				},
			},
		}
		r.client = mockClient
		r.authProvider = &mockAuthProvider{}

		err := r.Init(t.Context(), nameresolution.Metadata{
			Configuration: map[string]interface{}{
				"namespaceId": "ns-test",
				"region":      "us-west-2",
			},
		})

		require.NoError(t, err)
		assert.Equal(t, "ns-test", r.namespaceID)
		assert.Equal(t, "test-namespace", r.namespaceName)
		assert.Equal(t, defaultDaprPort, r.defaultDaprPort)
	})

	t.Run("init with custom default port", func(t *testing.T) {
		r := NewResolver(logger.NewLogger("test")).(*Resolver)
		mockClient := &mockServiceDiscoveryAPI{
			getNamespaceResp: &servicediscovery.GetNamespaceOutput{
				Namespace: &types.Namespace{
					Name: aws.String("test-namespace"),
				},
			},
		}
		r.client = mockClient
		r.authProvider = &mockAuthProvider{}

		err := r.Init(t.Context(), nameresolution.Metadata{
			Configuration: map[string]interface{}{
				"namespaceId":     "ns-test",
				"region":          "us-west-2",
				"defaultDaprPort": 5000,
			},
		})

		require.NoError(t, err)
		assert.Equal(t, 5000, r.defaultDaprPort)
	})

	t.Run("init with valid namespace name", func(t *testing.T) {
		r := NewResolver(logger.NewLogger("test")).(*Resolver)
		mockClient := &mockServiceDiscoveryAPI{
			listNamespacesResp: &servicediscovery.ListNamespacesOutput{
				Namespaces: []types.NamespaceSummary{
					{
						Name: aws.String("test-namespace"),
						Id:   aws.String("ns-test"),
					},
				},
			},
		}
		r.client = mockClient
		r.authProvider = &mockAuthProvider{}

		err := r.Init(t.Context(), nameresolution.Metadata{
			Configuration: map[string]interface{}{
				"namespaceName": "test-namespace",
				"region":        "us-west-2",
			},
		})

		require.NoError(t, err)
		assert.Equal(t, "ns-test", r.namespaceID)
		assert.Equal(t, "test-namespace", r.namespaceName)
	})

	t.Run("init with missing namespace", func(t *testing.T) {
		r := NewResolver(logger.NewLogger("test")).(*Resolver)
		err := r.Init(t.Context(), nameresolution.Metadata{
			Configuration: map[string]interface{}{
				"region": "us-west-2",
			},
		})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "either namespaceName or namespaceId must be provided")
	})

	t.Run("resolve service with healthy instances", func(t *testing.T) {
		r := NewResolver(logger.NewLogger("test")).(*Resolver)
		mockClient := &mockServiceDiscoveryAPI{
			discoverInstancesResp: &servicediscovery.DiscoverInstancesOutput{
				Instances: []types.HttpInstanceSummary{
					{
						InstanceId: aws.String("i-1234"),
						Attributes: map[string]string{
							"AWS_INSTANCE_IPV4": "10.0.0.1",
							"DAPR_PORT":         "8080",
						},
					},
				},
			},
		}
		r.client = mockClient
		r.namespaceName = "test-namespace"

		addr, err := r.ResolveID(t.Context(), nameresolution.ResolveRequest{
			ID: "test-service",
		})

		require.NoError(t, err)
		assert.Equal(t, "10.0.0.1:8080", addr)
	})

	t.Run("resolve service with no instances", func(t *testing.T) {
		r := NewResolver(logger.NewLogger("test")).(*Resolver)
		mockClient := &mockServiceDiscoveryAPI{
			discoverInstancesResp: &servicediscovery.DiscoverInstancesOutput{
				Instances: []types.HttpInstanceSummary{},
			},
		}
		r.client = mockClient
		r.namespaceName = "test-namespace"

		_, err := r.ResolveID(t.Context(), nameresolution.ResolveRequest{
			ID: "test-service",
		})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "no healthy instances found for service test-service")
	})

	t.Run("resolve service with discovery error", func(t *testing.T) {
		r := NewResolver(logger.NewLogger("test")).(*Resolver)
		mockClient := &mockServiceDiscoveryAPI{
			discoverInstancesErr: assert.AnError,
		}
		r.client = mockClient
		r.namespaceName = "test-namespace"

		_, err := r.ResolveID(t.Context(), nameresolution.ResolveRequest{
			ID: "test-service",
		})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to discover CloudMap instances")
	})

	t.Run("resolve service with DAPR_PORT", func(t *testing.T) {
		r := NewResolver(logger.NewLogger("test")).(*Resolver)
		mockClient := &mockServiceDiscoveryAPI{
			discoverInstancesResp: &servicediscovery.DiscoverInstancesOutput{
				Instances: []types.HttpInstanceSummary{
					{
						InstanceId: aws.String("i-1234"),
						Attributes: map[string]string{
							"AWS_INSTANCE_IPV4": "10.0.0.1",
							"DAPR_PORT":         "5000",
						},
					},
				},
			},
		}
		r.client = mockClient
		r.namespaceName = "test-namespace"

		addr, err := r.ResolveID(t.Context(), nameresolution.ResolveRequest{
			ID: "test-service",
		})

		require.NoError(t, err)
		assert.Equal(t, "10.0.0.1:5000", addr)
	})

	t.Run("resolve service with default port", func(t *testing.T) {
		r := NewResolver(logger.NewLogger("test")).(*Resolver)
		mockClient := &mockServiceDiscoveryAPI{
			discoverInstancesResp: &servicediscovery.DiscoverInstancesOutput{
				Instances: []types.HttpInstanceSummary{
					{
						InstanceId: aws.String("i-1234"),
						Attributes: map[string]string{
							"AWS_INSTANCE_IPV4": "10.0.0.1",
						},
					},
				},
			},
		}
		r.client = mockClient
		r.namespaceName = "test-namespace"
		r.defaultDaprPort = 3500

		addr, err := r.ResolveID(t.Context(), nameresolution.ResolveRequest{
			ID: "test-service",
		})

		require.NoError(t, err)
		assert.Equal(t, "10.0.0.1:3500", addr)
	})

	t.Run("close with auth provider", func(t *testing.T) {
		r := NewResolver(logger.NewLogger("test")).(*Resolver)
		mockAuthProvider := &mockAuthProvider{}
		r.authProvider = mockAuthProvider

		err := r.Close()

		require.NoError(t, err)
		assert.True(t, mockAuthProvider.closeCalled)
	})
}

func TestResolve(t *testing.T) {
	testCases := []struct {
		name              string
		req               nameresolution.ResolveRequest
		mockResponse      *servicediscovery.DiscoverInstancesOutput
		mockError         error
		defaultPort       int
		expectedAddresses []string
		expectedError     bool
	}{
		{
			name: "successful resolution with DAPR_PORT",
			req: nameresolution.ResolveRequest{
				ID: "test-service",
			},
			mockResponse: &servicediscovery.DiscoverInstancesOutput{
				Instances: []types.HttpInstanceSummary{
					{
						InstanceId: aws.String("i-1234"),
						Attributes: map[string]string{
							"AWS_INSTANCE_IPV4": "192.0.2.1",
							"DAPR_PORT":         "5000",
						},
					},
					{
						InstanceId: aws.String("i-5678"),
						Attributes: map[string]string{
							"AWS_INSTANCE_IPV4": "192.0.2.2",
							"DAPR_PORT":         "5000",
						},
					},
				},
			},
			expectedAddresses: []string{"192.0.2.1:5000", "192.0.2.2:5000"},
			expectedError:     false,
		},
		{
			name: "successful resolution with default port",
			req: nameresolution.ResolveRequest{
				ID: "test-service",
			},
			defaultPort: 3500,
			mockResponse: &servicediscovery.DiscoverInstancesOutput{
				Instances: []types.HttpInstanceSummary{
					{
						InstanceId: aws.String("i-1234"),
						Attributes: map[string]string{
							"AWS_INSTANCE_IPV4": "192.0.2.1",
						},
					},
					{
						InstanceId: aws.String("i-5678"),
						Attributes: map[string]string{
							"AWS_INSTANCE_IPV4": "192.0.2.2",
						},
					},
				},
			},
			expectedAddresses: []string{"192.0.2.1:3500", "192.0.2.2:3500"},
			expectedError:     false,
		},
		{
			name: "error from AWS",
			req: nameresolution.ResolveRequest{
				ID: "test-service",
			},
			mockError:         errors.New("AWS error"),
			expectedAddresses: nil,
			expectedError:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := &mockServiceDiscoveryAPI{
				discoverInstancesResp: tc.mockResponse,
				discoverInstancesErr:  tc.mockError,
			}

			resolver := &Resolver{
				client:          mockClient,
				logger:          logger.NewLogger("test"),
				namespaceName:   "test-namespace",
				defaultDaprPort: tc.defaultPort,
			}

			addresses, err := resolver.ResolveID(t.Context(), tc.req)

			if tc.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if len(tc.expectedAddresses) == 1 {
					assert.Equal(t, tc.expectedAddresses[0], addresses)
				} else {
					assert.Contains(t, tc.expectedAddresses, addresses)
				}
			}
		})
	}
}
