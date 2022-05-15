/*
Copyright 2021 The Dapr Authors
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

package consul

import (
	"fmt"
	"strconv"
	"testing"

	consul "github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"

	nr "github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
)

type mockClient struct {
	mockAgent
	mockHealth

	initClientCalled int
	initClientErr    error
}

func (m *mockClient) InitClient(config *consul.Config) error {
	m.initClientCalled++

	return m.initClientErr
}

func (m *mockClient) Health() healthInterface {
	return &m.mockHealth
}

func (m *mockClient) Agent() agentInterface {
	return &m.mockAgent
}

type mockHealth struct {
	serviceCalled int
	serviceErr    error
	serviceResult []*consul.ServiceEntry
	serviceMeta   *consul.QueryMeta
}

func (m *mockHealth) Service(service, tag string, passingOnly bool, q *consul.QueryOptions) ([]*consul.ServiceEntry, *consul.QueryMeta, error) {
	m.serviceCalled++

	return m.serviceResult, m.serviceMeta, m.serviceErr
}

type mockAgent struct {
	selfCalled            int
	selfErr               error
	selfResult            map[string]map[string]interface{}
	serviceRegisterCalled int
	serviceRegisterErr    error
}

func (m *mockAgent) Self() (map[string]map[string]interface{}, error) {
	m.selfCalled++

	return m.selfResult, m.selfErr
}

func (m *mockAgent) ServiceRegister(service *consul.AgentServiceRegistration) error {
	m.serviceRegisterCalled++

	return m.serviceRegisterErr
}

func TestInit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		testName string
		metadata nr.Metadata
		test     func(*testing.T, nr.Metadata)
	}{
		{
			"given no configuration don't register service just check agent",
			nr.Metadata{
				Properties:    getTestPropsWithoutKey(""),
				Configuration: nil,
			},
			func(t *testing.T, metadata nr.Metadata) {
				t.Helper()

				var mock mockClient
				resolver := newResolver(logger.NewLogger("test"), resolverConfig{}, &mock)

				_ = resolver.Init(metadata)

				assert.Equal(t, 1, mock.initClientCalled)
				assert.Equal(t, 0, mock.mockAgent.serviceRegisterCalled)
				assert.Equal(t, 1, mock.mockAgent.selfCalled)
			},
		},
		{
			"given SelfRegister true then register service",
			nr.Metadata{
				Properties: getTestPropsWithoutKey(""),
				Configuration: configSpec{
					SelfRegister: true,
				},
			},
			func(t *testing.T, metadata nr.Metadata) {
				t.Helper()

				var mock mockClient
				resolver := newResolver(logger.NewLogger("test"), resolverConfig{}, &mock)

				_ = resolver.Init(metadata)

				assert.Equal(t, 1, mock.initClientCalled)
				assert.Equal(t, 1, mock.mockAgent.serviceRegisterCalled)
				assert.Equal(t, 0, mock.mockAgent.selfCalled)
			},
		},
		{
			"given AdvancedRegistraion then register service",
			nr.Metadata{
				Properties: getTestPropsWithoutKey(""),
				Configuration: configSpec{
					AdvancedRegistration: &consul.AgentServiceRegistration{},
					QueryOptions:         &consul.QueryOptions{},
				},
			},
			func(t *testing.T, metadata nr.Metadata) {
				t.Helper()

				var mock mockClient
				resolver := newResolver(logger.NewLogger("test"), resolverConfig{}, &mock)

				_ = resolver.Init(metadata)

				assert.Equal(t, 1, mock.initClientCalled)
				assert.Equal(t, 1, mock.mockAgent.serviceRegisterCalled)
				assert.Equal(t, 0, mock.mockAgent.selfCalled)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.testName, func(t *testing.T) {
			t.Parallel()
			tt.test(t, tt.metadata)
		})
	}
}

func TestResolveID(t *testing.T) {
	t.Parallel()
	testConfig := &resolverConfig{
		DaprPortMetaKey: "DAPR_PORT",
	}

	tests := []struct {
		testName string
		req      nr.ResolveRequest
		test     func(*testing.T, nr.ResolveRequest)
	}{
		{
			"error if no healthy services found",
			nr.ResolveRequest{
				ID: "test-app",
			},
			func(t *testing.T, req nr.ResolveRequest) {
				t.Helper()
				mock := mockClient{
					mockHealth: mockHealth{
						serviceResult: []*consul.ServiceEntry{},
					},
				}
				resolver := newResolver(logger.NewLogger("test"), *testConfig, &mock)

				_, err := resolver.ResolveID(req)
				assert.Equal(t, 1, mock.mockHealth.serviceCalled)
				assert.Error(t, err)
			},
		},
		{
			"should get address from service",
			nr.ResolveRequest{
				ID: "test-app",
			},
			func(t *testing.T, req nr.ResolveRequest) {
				t.Helper()
				mock := mockClient{
					mockHealth: mockHealth{
						serviceResult: []*consul.ServiceEntry{
							{
								Service: &consul.AgentService{
									Address: "123.234.345.456",
									Port:    8600,
									Meta: map[string]string{
										"DAPR_PORT": "50005",
									},
								},
							},
						},
					},
				}
				resolver := newResolver(logger.NewLogger("test"), *testConfig, &mock)

				addr, _ := resolver.ResolveID(req)

				assert.Equal(t, "123.234.345.456:50005", addr)
			},
		},
		{
			"should get address from node if not on service",
			nr.ResolveRequest{
				ID: "test-app",
			},
			func(t *testing.T, req nr.ResolveRequest) {
				t.Helper()
				mock := mockClient{
					mockHealth: mockHealth{
						serviceResult: []*consul.ServiceEntry{
							{
								Node: &consul.Node{
									Address: "999.888.777",
								},
								Service: &consul.AgentService{
									Address: "",
									Port:    8600,
									Meta: map[string]string{
										"DAPR_PORT": "50005",
									},
								},
							},
							{
								Node: &consul.Node{
									Address: "999.888.777",
								},
								Service: &consul.AgentService{
									Address: "",
									Port:    8600,
									Meta: map[string]string{
										"DAPR_PORT": "50005",
									},
								},
							},
						},
					},
				}
				resolver := newResolver(logger.NewLogger("test"), *testConfig, &mock)

				addr, _ := resolver.ResolveID(req)

				assert.Equal(t, "999.888.777:50005", addr)
			},
		},
		{
			"error if no address found on service",
			nr.ResolveRequest{
				ID: "test-app",
			},
			func(t *testing.T, req nr.ResolveRequest) {
				t.Helper()
				mock := mockClient{
					mockHealth: mockHealth{
						serviceResult: []*consul.ServiceEntry{
							{
								Node: &consul.Node{},
								Service: &consul.AgentService{
									Port: 8600,
									Meta: map[string]string{
										"DAPR_PORT": "50005",
									},
								},
							},
						},
					},
				}
				resolver := newResolver(logger.NewLogger("test"), *testConfig, &mock)

				_, err := resolver.ResolveID(req)

				assert.Error(t, err)
			},
		},
		{
			"error if consul service missing DaprPortMetaKey",
			nr.ResolveRequest{
				ID: "test-app",
			},
			func(t *testing.T, req nr.ResolveRequest) {
				t.Helper()
				mock := mockClient{
					mockHealth: mockHealth{
						serviceResult: []*consul.ServiceEntry{
							{
								Service: &consul.AgentService{
									Address: "123.234.345.456",
									Port:    8600,
								},
							},
						},
					},
				}
				resolver := newResolver(logger.NewLogger("test"), *testConfig, &mock)

				_, err := resolver.ResolveID(req)

				assert.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.testName, func(t *testing.T) {
			t.Parallel()
			tt.test(t, tt.req)
		})
	}
}

func TestParseConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		testName    string
		shouldParse bool
		input       interface{}
		expected    configSpec
	}{
		{
			"valid configuration in metadata",
			true,
			map[interface{}]interface{}{
				"Checks": []interface{}{
					map[interface{}]interface{}{
						"Name":     "test-app health check name",
						"CheckID":  "test-app health check id",
						"Interval": "15s",
						"HTTP":     "http://127.0.0.1:3500/health",
					},
				},
				"Tags": []interface{}{
					"dapr",
					"test",
				},
				"Meta": map[interface{}]interface{}{
					"APP_PORT":       "123",
					"DAPR_HTTP_PORT": "3500",
					"DAPR_GRPC_PORT": "50005",
				},
				"QueryOptions": map[interface{}]interface{}{
					"UseCache": true,
					"Filter":   "Checks.ServiceTags contains dapr",
				},
			},
			configSpec{
				Checks: []*consul.AgentServiceCheck{
					{
						Name:     "test-app health check name",
						CheckID:  "test-app health check id",
						Interval: "15s",
						HTTP:     "http://127.0.0.1:3500/health",
					},
				},
				Tags: []string{
					"dapr",
					"test",
				},
				Meta: map[string]string{
					"APP_PORT":       "123",
					"DAPR_HTTP_PORT": "3500",
					"DAPR_GRPC_PORT": "50005",
				},
				QueryOptions: &consul.QueryOptions{
					UseCache: true,
					Filter:   "Checks.ServiceTags contains dapr",
				},
			},
		},
		{
			"invalid configuration in metadata",
			false,
			map[interface{}]interface{}{
				"Checks": []interface{}{
					map[interface{}]interface{}{
						"Name":     "health check name",
						"IAMFAKE":  "health check id",
						"Interval": "15s",
						"HTTP":     "http://127.0.0.1:3500/health",
					},
				},
				"Bob": []interface{}{
					"dapr",
					"test",
				},
				"Meta": map[interface{}]interface{}{
					"DAPR_HTTP_PORT": "3500",
					"DAPR_GRPC_PORT": "50005",
				},
				"QueryOptions": map[interface{}]interface{}{
					"NOTAREALFIELDNAME": true,
					"Filter":            "Checks.ServiceTags contains dapr",
				},
			},
			configSpec{},
		},
		{
			"empty configuration in metadata",
			true,
			nil,
			configSpec{},
		},
		{
			"fail on unsupported map key",
			false,
			map[interface{}]interface{}{
				1000: map[interface{}]interface{}{
					"DAPR_HTTP_PORT": "3500",
					"DAPR_GRPC_PORT": "50005",
				},
			},
			configSpec{},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.testName, func(t *testing.T) {
			t.Parallel()
			actual, err := parseConfig(tt.input)

			if tt.shouldParse {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, actual)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestGetConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		testName string
		metadata nr.Metadata
		test     func(*testing.T, nr.Metadata)
	}{
		{
			"empty configuration should only return Client, QueryOptions and DaprPortMetaKey",
			nr.Metadata{
				Properties:    getTestPropsWithoutKey(""),
				Configuration: nil,
			},
			func(t *testing.T, metadata nr.Metadata) {
				t.Helper()
				actual, _ := getConfig(metadata)

				// Client
				assert.Equal(t, consul.DefaultConfig().Address, actual.Client.Address)

				// Registration
				assert.Nil(t, actual.Registration)

				// QueryOptions
				assert.NotNil(t, actual.QueryOptions)
				assert.Equal(t, true, actual.QueryOptions.UseCache)

				// DaprPortMetaKey
				assert.Equal(t, "DAPR_PORT", actual.DaprPortMetaKey)
			},
		},
		{
			"empty configuration with SelfRegister should default correctly",
			nr.Metadata{
				Properties: getTestPropsWithoutKey(""),
				Configuration: configSpec{
					SelfRegister: true,
				},
			},
			func(t *testing.T, metadata nr.Metadata) {
				t.Helper()
				actual, _ := getConfig(metadata)
				// Client
				assert.Equal(t, consul.DefaultConfig().Address, actual.Client.Address)

				// Checks
				assert.Equal(t, 1, len(actual.Registration.Checks))
				check := actual.Registration.Checks[0]
				assert.Equal(t, "Dapr Health Status", check.Name)
				// assert.Equal(t, "daprHealth:test-app", check.CheckID)
				assert.Equal(t, "15s", check.Interval)
				assert.Equal(t, fmt.Sprintf("http://%s:%s/v1.0/healthz", metadata.Properties[nr.HostAddress], metadata.Properties[nr.DaprHTTPPort]), check.HTTP)

				// Metadata
				assert.Equal(t, 1, len(actual.Registration.Meta))
				assert.Equal(t, "50001", actual.Registration.Meta["DAPR_PORT"])

				// QueryOptions
				assert.Equal(t, true, actual.QueryOptions.UseCache)

				// DaprPortMetaKey
				assert.Equal(t, "DAPR_PORT", actual.DaprPortMetaKey)
			},
		},
		{
			"DaprPortMetaKey should set registration meta and config used for resolve",
			nr.Metadata{
				Properties: getTestPropsWithoutKey(""),
				Configuration: configSpec{
					SelfRegister:    true,
					DaprPortMetaKey: "random_key",
				},
			},
			func(t *testing.T, metadata nr.Metadata) {
				t.Helper()
				actual, _ := getConfig(metadata)

				daprPort := metadata.Properties[nr.DaprPort]

				assert.Equal(t, "random_key", actual.DaprPortMetaKey)
				assert.Equal(t, daprPort, actual.Registration.Meta["random_key"])
			},
		},
		{
			"missing AppID property should error when SelfRegister true",
			nr.Metadata{
				Properties: getTestPropsWithoutKey(nr.AppID),
				Configuration: configSpec{
					SelfRegister: true,
				},
			},
			func(t *testing.T, metadata nr.Metadata) {
				t.Helper()
				_, err := getConfig(metadata)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), nr.AppID)

				metadata.Configuration = configSpec{
					SelfRegister: false,
				}

				_, err = getConfig(metadata)
				assert.NoError(t, err)

				metadata.Configuration = configSpec{
					AdvancedRegistration: &consul.AgentServiceRegistration{},
					QueryOptions:         &consul.QueryOptions{},
				}

				_, err = getConfig(metadata)
				assert.NoError(t, err)
			},
		},
		{
			"missing AppPort property should error when SelfRegister true",
			nr.Metadata{
				Properties: getTestPropsWithoutKey(nr.AppPort),
				Configuration: configSpec{
					SelfRegister: true,
				},
			},
			func(t *testing.T, metadata nr.Metadata) {
				t.Helper()
				_, err := getConfig(metadata)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), nr.AppPort)

				metadata.Configuration = configSpec{
					SelfRegister: false,
				}

				_, err = getConfig(metadata)
				assert.NoError(t, err)

				metadata.Configuration = configSpec{
					AdvancedRegistration: &consul.AgentServiceRegistration{},
					QueryOptions:         &consul.QueryOptions{},
				}

				_, err = getConfig(metadata)
				assert.NoError(t, err)
			},
		},
		{
			"missing HostAddress property should error when SelfRegister true",
			nr.Metadata{
				Properties: getTestPropsWithoutKey(nr.HostAddress),
				Configuration: configSpec{
					SelfRegister: true,
				},
			},
			func(t *testing.T, metadata nr.Metadata) {
				t.Helper()
				_, err := getConfig(metadata)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), nr.HostAddress)

				metadata.Configuration = configSpec{
					SelfRegister: false,
				}

				_, err = getConfig(metadata)
				assert.NoError(t, err)

				metadata.Configuration = configSpec{
					AdvancedRegistration: &consul.AgentServiceRegistration{},
					QueryOptions:         &consul.QueryOptions{},
				}

				_, err = getConfig(metadata)
				assert.NoError(t, err)
			},
		},
		{
			"missing DaprHTTPPort property should error only when SelfRegister true",
			nr.Metadata{
				Properties: getTestPropsWithoutKey(nr.DaprHTTPPort),
				Configuration: configSpec{
					SelfRegister: true,
				},
			},
			func(t *testing.T, metadata nr.Metadata) {
				t.Helper()
				_, err := getConfig(metadata)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), nr.DaprHTTPPort)

				metadata.Configuration = configSpec{
					SelfRegister: false,
				}

				_, err = getConfig(metadata)
				assert.NoError(t, err)

				metadata.Configuration = configSpec{
					AdvancedRegistration: &consul.AgentServiceRegistration{},
					QueryOptions:         &consul.QueryOptions{},
				}

				_, err = getConfig(metadata)
				assert.NoError(t, err)
			},
		},
		{
			"missing DaprPort property should always error",
			nr.Metadata{
				Properties: getTestPropsWithoutKey(nr.DaprPort),
			},
			func(t *testing.T, metadata nr.Metadata) {
				t.Helper()
				metadata.Configuration = configSpec{
					SelfRegister: false,
				}

				_, err := getConfig(metadata)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), nr.DaprPort)

				metadata.Configuration = configSpec{
					SelfRegister: true,
				}

				_, err = getConfig(metadata)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), nr.DaprPort)

				metadata.Configuration = configSpec{
					AdvancedRegistration: &consul.AgentServiceRegistration{},
					QueryOptions:         &consul.QueryOptions{},
				}

				_, err = getConfig(metadata)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), nr.DaprPort)
			},
		},
		{
			"registration should configure correctly",
			nr.Metadata{
				Properties: getTestPropsWithoutKey(""),
				Configuration: configSpec{
					Checks: []*consul.AgentServiceCheck{
						{
							Name:     "test-app health check name",
							CheckID:  "test-app health check id",
							Interval: "15s",
							HTTP:     "http://127.0.0.1:3500/health",
						},
					},
					Tags: []string{
						"test",
					},
					Meta: map[string]string{
						"APP_PORT":       "8650",
						"DAPR_GRPC_PORT": "50005",
					},
					QueryOptions: &consul.QueryOptions{
						UseCache: false,
						Filter:   "Checks.ServiceTags contains something",
					},
					SelfRegister: true,
				},
			},
			func(t *testing.T, metadata nr.Metadata) {
				t.Helper()
				actual, _ := getConfig(metadata)

				appPort, _ := strconv.Atoi(metadata.Properties[nr.AppPort])

				// Enabled Registration
				assert.NotNil(t, actual.Registration)
				assert.Equal(t, metadata.Properties[nr.AppID], actual.Registration.Name)
				assert.Equal(t, metadata.Properties[nr.HostAddress], actual.Registration.Address)
				assert.Equal(t, appPort, actual.Registration.Port)
				assert.Equal(t, "test-app health check name", actual.Registration.Checks[0].Name)
				assert.Equal(t, "test-app health check id", actual.Registration.Checks[0].CheckID)
				assert.Equal(t, "15s", actual.Registration.Checks[0].Interval)
				assert.Equal(t, "http://127.0.0.1:3500/health", actual.Registration.Checks[0].HTTP)
				assert.Equal(t, "test", actual.Registration.Tags[0])
				assert.Equal(t, "8650", actual.Registration.Meta["APP_PORT"])
				assert.Equal(t, "50005", actual.Registration.Meta["DAPR_GRPC_PORT"])
				assert.Equal(t, false, actual.QueryOptions.UseCache)
				assert.Equal(t, "Checks.ServiceTags contains something", actual.QueryOptions.Filter)
			},
		},
		{
			"advanced registration should override/ignore other configs",
			nr.Metadata{
				Properties: getTestPropsWithoutKey(""),
				Configuration: configSpec{
					AdvancedRegistration: &consul.AgentServiceRegistration{
						Name:    "random-app-id",
						Port:    0o00,
						Address: "123.345.678",
						Tags:    []string{"random-tag"},
						Meta: map[string]string{
							"APP_PORT": "000",
						},
						Checks: []*consul.AgentServiceCheck{
							{
								Name:     "random health check name",
								CheckID:  "random health check id",
								Interval: "15s",
								HTTP:     "http://127.0.0.1:3500/health",
							},
						},
					},
					Checks: []*consul.AgentServiceCheck{
						{
							Name:     "test-app health check name",
							CheckID:  "test-app health check id",
							Interval: "15s",
							HTTP:     "http://127.0.0.1:3500/health",
						},
					},
					Tags: []string{
						"dapr",
						"test",
					},
					Meta: map[string]string{
						"APP_PORT":       "123",
						"DAPR_HTTP_PORT": "3500",
						"DAPR_GRPC_PORT": "50005",
					},
					SelfRegister: false,
				},
			},
			func(t *testing.T, metadata nr.Metadata) {
				t.Helper()
				actual, _ := getConfig(metadata)

				// Enabled Registration
				assert.NotNil(t, actual.Registration)
				assert.Equal(t, "random-app-id", actual.Registration.Name)
				assert.Equal(t, "123.345.678", actual.Registration.Address)
				assert.Equal(t, 0o00, actual.Registration.Port)
				assert.Equal(t, "random health check name", actual.Registration.Checks[0].Name)
				assert.Equal(t, "000", actual.Registration.Meta["APP_PORT"])
				assert.Equal(t, "random-tag", actual.Registration.Tags[0])
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.testName, func(t *testing.T) {
			t.Parallel()
			tt.test(t, tt.metadata)
		})
	}
}

func TestMapConfig(t *testing.T) {
	t.Parallel()

	t.Run("should map full configuration", func(t *testing.T) {
		t.Helper()

		expected := intermediateConfig{
			Client: &Config{
				Address:    "Address",
				Scheme:     "Scheme",
				Datacenter: "Datacenter",
				HTTPAuth: &HTTPBasicAuth{
					Username: "Username",
					Password: "Password",
				},
				WaitTime:  10,
				Token:     "Token",
				TokenFile: "TokenFile",
				TLSConfig: TLSConfig{
					Address:            "Address",
					CAFile:             "CAFile",
					CAPath:             "CAPath",
					CertFile:           "CertFile",
					KeyFile:            "KeyFile",
					InsecureSkipVerify: true,
				},
			},
			Checks: []*AgentServiceCheck{
				{
					Args: []string{
						"arg1",
						"arg2",
					},
					CheckID:                        "CheckID",
					Name:                           "Name",
					DockerContainerID:              "DockerContainerID",
					Shell:                          "Shell",
					Interval:                       "Interval",
					Timeout:                        "Timeout",
					TTL:                            "TTL",
					HTTP:                           "HTTP",
					Method:                         "Method",
					TCP:                            "TCP",
					Status:                         "Status",
					Notes:                          "Notes",
					GRPC:                           "GRPC",
					AliasNode:                      "AliasNode",
					AliasService:                   "AliasService",
					DeregisterCriticalServiceAfter: "DeregisterCriticalServiceAfter",
					Header: map[string][]string{
						"M":  {"Key", "Value"},
						"M2": {"Key2", "Value2"},
					},
					TLSSkipVerify: true,
					GRPCUseTLS:    true,
				},
				{
					Args: []string{
						"arg1",
						"arg2",
					},
					CheckID:                        "CheckID2",
					Name:                           "Name2",
					DockerContainerID:              "DockerContainerID2",
					Shell:                          "Shell2",
					Interval:                       "Interval2",
					Timeout:                        "Timeout2",
					TTL:                            "TTL2",
					HTTP:                           "HTTP2",
					Method:                         "Method2",
					TCP:                            "TCP2",
					Status:                         "Status2",
					Notes:                          "Notes2",
					GRPC:                           "GRPC2",
					AliasNode:                      "AliasNode2",
					AliasService:                   "AliasService2",
					DeregisterCriticalServiceAfter: "DeregisterCriticalServiceAfter2",
					Header: map[string][]string{
						"M":  {"Key", "Value"},
						"M2": {"Key2", "Value2"},
					},
					TLSSkipVerify: true,
					GRPCUseTLS:    true,
				},
			},
			Tags: []string{
				"tag1",
				"tag2",
			},
			Meta: map[string]string{
				"M":  "Value",
				"M2": "Value2",
			},
			QueryOptions: &QueryOptions{
				Datacenter:   "Datacenter",
				WaitHash:     "WaitHash",
				Token:        "Token",
				Near:         "Near",
				Filter:       "Filter",
				MaxAge:       11,
				StaleIfError: 22,
				WaitIndex:    33,
				WaitTime:     44,
				NodeMeta: map[string]string{
					"M":  "Value",
					"M2": "Value2",
				},
				AllowStale:        true,
				RequireConsistent: true,
				UseCache:          true,
				RelayFactor:       55,
				LocalOnly:         true,
				Connect:           true,
			},
			AdvancedRegistration: &AgentServiceRegistration{
				Kind: "Kind",
				ID:   "ID",
				Name: "Name",
				Tags: []string{
					"tag1",
					"tag2",
				},
				Meta: map[string]string{
					"M":  "Value",
					"M2": "Value2",
				},
				Port:    123456,
				Address: "Address",
				TaggedAddresses: map[string]ServiceAddress{
					"T1": {
						Address: "Address",
						Port:    234567,
					},
					"T2": {
						Address: "Address",
						Port:    345678,
					},
				},
				EnableTagOverride: true,
				Weights: &AgentWeights{
					Passing: 123,
					Warning: 321,
				},
				Check: &AgentServiceCheck{
					Args: []string{
						"arg1",
						"arg2",
					},
					CheckID:                        "CheckID2",
					Name:                           "Name2",
					DockerContainerID:              "DockerContainerID2",
					Shell:                          "Shell2",
					Interval:                       "Interval2",
					Timeout:                        "Timeout2",
					TTL:                            "TTL2",
					HTTP:                           "HTTP2",
					Method:                         "Method2",
					TCP:                            "TCP2",
					Status:                         "Status2",
					Notes:                          "Notes2",
					GRPC:                           "GRPC2",
					AliasNode:                      "AliasNode2",
					AliasService:                   "AliasService2",
					DeregisterCriticalServiceAfter: "DeregisterCriticalServiceAfter2",
					Header: map[string][]string{
						"M":  {"Key", "Value"},
						"M2": {"Key2", "Value2"},
					},
					TLSSkipVerify: true,
					GRPCUseTLS:    true,
				},
				Checks: AgentServiceChecks{
					{
						Args: []string{
							"arg1",
							"arg2",
						},
						CheckID:                        "CheckID",
						Name:                           "Name",
						DockerContainerID:              "DockerContainerID",
						Shell:                          "Shell",
						Interval:                       "Interval",
						Timeout:                        "Timeout",
						TTL:                            "TTL",
						HTTP:                           "HTTP",
						Method:                         "Method",
						TCP:                            "TCP",
						Status:                         "Status",
						Notes:                          "Notes",
						GRPC:                           "GRPC",
						AliasNode:                      "AliasNode",
						AliasService:                   "AliasService",
						DeregisterCriticalServiceAfter: "DeregisterCriticalServiceAfter",
						Header: map[string][]string{
							"M":  {"Key", "Value"},
							"M2": {"Key2", "Value2"},
						},
						TLSSkipVerify: true,
						GRPCUseTLS:    true,
					},
					{
						Args: []string{
							"arg1",
							"arg2",
						},
						CheckID:                        "CheckID2",
						Name:                           "Name2",
						DockerContainerID:              "DockerContainerID2",
						Shell:                          "Shell2",
						Interval:                       "Interval2",
						Timeout:                        "Timeout2",
						TTL:                            "TTL2",
						HTTP:                           "HTTP2",
						Method:                         "Method2",
						TCP:                            "TCP2",
						Status:                         "Status2",
						Notes:                          "Notes2",
						GRPC:                           "GRPC2",
						AliasNode:                      "AliasNode2",
						AliasService:                   "AliasService2",
						DeregisterCriticalServiceAfter: "DeregisterCriticalServiceAfter2",
						Header: map[string][]string{
							"M":  {"Key", "Value"},
							"M2": {"Key2", "Value2"},
						},
						TLSSkipVerify: true,
						GRPCUseTLS:    true,
					},
				},
				Proxy: &AgentServiceConnectProxyConfig{
					DestinationServiceName: "DestinationServiceName",
					DestinationServiceID:   "DestinationServiceID",
					LocalServiceAddress:    "LocalServiceAddress",
					LocalServicePort:       123456,
					Config: map[string]interface{}{
						"Random": 123123,
					},
					Upstreams: []Upstream{
						{
							DestinationType:      "DestinationType",
							DestinationNamespace: "DestinationNamespace",
							DestinationName:      "DestinationName",
							Datacenter:           "Datacenter",
							LocalBindAddress:     "LocalBindAddress",
							LocalBindPort:        234567,
							Config: map[string]interface{}{
								"Random": 321321,
							},
							MeshGateway: MeshGatewayConfig{
								Mode: "Mode",
							},
						},
					},
					MeshGateway: MeshGatewayConfig{
						Mode: "Mode2",
					},
					Expose: ExposeConfig{
						Checks: true,
						Paths: []ExposePath{
							{
								ListenerPort:    123456,
								Path:            "Path",
								LocalPathPort:   234567,
								Protocol:        "Protocol",
								ParsedFromCheck: true,
							},
						},
					},
				},
				Connect: &AgentServiceConnect{
					Native:         true,
					SidecarService: nil,
				},
			},
			SelfRegister:    true,
			DaprPortMetaKey: "SOMETHINGSOMETHING",
		}

		actual := mapConfig(expected)

		compareQueryOptions(t, expected.QueryOptions, actual.QueryOptions)
		compareRegistration(t, expected.AdvancedRegistration, actual.AdvancedRegistration)
		compareClientConfig(t, expected.Client, actual.Client)

		for i := 0; i < len(expected.Checks); i++ {
			compareCheck(t, expected.Checks[i], actual.Checks[i])
		}

		assert.Equal(t, expected.Tags, actual.Tags)
		assert.Equal(t, expected.Meta, actual.Meta)
		assert.Equal(t, expected.SelfRegister, actual.SelfRegister)
		assert.Equal(t, expected.DaprPortMetaKey, actual.DaprPortMetaKey)
	})

	t.Run("should map empty configuration", func(t *testing.T) {
		t.Helper()

		expected := intermediateConfig{}

		actual := mapConfig(expected)

		assert.Equal(t, configSpec{}, actual)
	})
}

func compareQueryOptions(t *testing.T, expected *QueryOptions, actual *consul.QueryOptions) {
	assert.Equal(t, expected.Datacenter, actual.Datacenter)
	assert.Equal(t, expected.AllowStale, actual.AllowStale)
	assert.Equal(t, expected.RequireConsistent, actual.RequireConsistent)
	assert.Equal(t, expected.UseCache, actual.UseCache)
	assert.Equal(t, expected.MaxAge, actual.MaxAge)
	assert.Equal(t, expected.StaleIfError, actual.StaleIfError)
	assert.Equal(t, expected.WaitIndex, actual.WaitIndex)
	assert.Equal(t, expected.WaitHash, actual.WaitHash)
	assert.Equal(t, expected.WaitTime, actual.WaitTime)
	assert.Equal(t, expected.Token, actual.Token)
	assert.Equal(t, expected.Near, actual.Near)
	assert.Equal(t, expected.NodeMeta, actual.NodeMeta)
	assert.Equal(t, expected.RelayFactor, actual.RelayFactor)
	assert.Equal(t, expected.LocalOnly, actual.LocalOnly)
	assert.Equal(t, expected.Connect, actual.Connect)
	assert.Equal(t, expected.Filter, actual.Filter)
}

func compareClientConfig(t *testing.T, expected *Config, actual *consul.Config) {
	assert.Equal(t, expected.Address, actual.Address)
	assert.Equal(t, expected.Datacenter, actual.Datacenter)

	if expected.HTTPAuth != nil {
		assert.Equal(t, expected.HTTPAuth.Username, actual.HttpAuth.Username)
		assert.Equal(t, expected.HTTPAuth.Password, actual.HttpAuth.Password)
	}

	assert.Equal(t, expected.Scheme, actual.Scheme)

	assert.Equal(t, expected.TLSConfig.Address, actual.TLSConfig.Address)
	assert.Equal(t, expected.TLSConfig.CAFile, actual.TLSConfig.CAFile)
	assert.Equal(t, expected.TLSConfig.CAPath, actual.TLSConfig.CAPath)
	assert.Equal(t, expected.TLSConfig.CertFile, actual.TLSConfig.CertFile)
	assert.Equal(t, expected.TLSConfig.InsecureSkipVerify, actual.TLSConfig.InsecureSkipVerify)
	assert.Equal(t, expected.TLSConfig.KeyFile, actual.TLSConfig.KeyFile)

	assert.Equal(t, expected.Token, actual.Token)
	assert.Equal(t, expected.TokenFile, actual.TokenFile)
	assert.Equal(t, expected.WaitTime, actual.WaitTime)
}

func compareRegistration(t *testing.T, expected *AgentServiceRegistration, actual *consul.AgentServiceRegistration) {
	assert.Equal(t, expected.Kind, string(actual.Kind))
	assert.Equal(t, expected.ID, actual.ID)
	assert.Equal(t, expected.Name, actual.Name)
	assert.Equal(t, expected.Tags, actual.Tags)
	assert.Equal(t, expected.Port, actual.Port)
	assert.Equal(t, expected.Address, actual.Address)

	if expected.TaggedAddresses != nil {
		for k := range expected.TaggedAddresses {
			assert.Equal(t, expected.TaggedAddresses[k].Address, actual.TaggedAddresses[k].Address)
			assert.Equal(t, expected.TaggedAddresses[k].Port, actual.TaggedAddresses[k].Port)
		}
	}

	assert.Equal(t, expected.EnableTagOverride, actual.EnableTagOverride)
	assert.Equal(t, expected.Meta, actual.Meta)

	if expected.Weights != nil {
		assert.Equal(t, expected.Weights.Passing, actual.Weights.Passing)
		assert.Equal(t, expected.Weights.Warning, actual.Weights.Warning)
	}

	compareCheck(t, expected.Check, actual.Check)

	for i := 0; i < len(expected.Checks); i++ {
		compareCheck(t, expected.Checks[i], actual.Checks[i])
	}

	if expected.Proxy != nil {
		assert.Equal(t, expected.Proxy.DestinationServiceName, actual.Proxy.DestinationServiceName)
		assert.Equal(t, expected.Proxy.DestinationServiceID, actual.Proxy.DestinationServiceID)
		assert.Equal(t, expected.Proxy.LocalServiceAddress, actual.Proxy.LocalServiceAddress)
		assert.Equal(t, expected.Proxy.LocalServicePort, actual.Proxy.LocalServicePort)
		assert.Equal(t, expected.Proxy.Config, actual.Proxy.Config)

		for i := 0; i < len(expected.Proxy.Upstreams); i++ {
			assert.Equal(t, string(expected.Proxy.Upstreams[i].DestinationType), string(actual.Proxy.Upstreams[i].DestinationType))
			assert.Equal(t, expected.Proxy.Upstreams[i].DestinationNamespace, actual.Proxy.Upstreams[i].DestinationNamespace)
			assert.Equal(t, expected.Proxy.Upstreams[i].DestinationName, actual.Proxy.Upstreams[i].DestinationName)
			assert.Equal(t, expected.Proxy.Upstreams[i].Datacenter, actual.Proxy.Upstreams[i].Datacenter)
			assert.Equal(t, expected.Proxy.Upstreams[i].LocalBindAddress, actual.Proxy.Upstreams[i].LocalBindAddress)
			assert.Equal(t, expected.Proxy.Upstreams[i].LocalBindPort, actual.Proxy.Upstreams[i].LocalBindPort)
			assert.Equal(t, expected.Proxy.Upstreams[i].Config, actual.Proxy.Upstreams[i].Config)
			assert.Equal(t, string(expected.Proxy.Upstreams[i].MeshGateway.Mode), string(actual.Proxy.Upstreams[i].MeshGateway.Mode))
		}

		assert.Equal(t, string(expected.Proxy.MeshGateway.Mode), string(actual.Proxy.MeshGateway.Mode))

		assert.Equal(t, expected.Proxy.Expose.Checks, actual.Proxy.Expose.Checks)

		for i := 0; i < len(expected.Proxy.Expose.Paths); i++ {
			assert.Equal(t, expected.Proxy.Expose.Paths[i].ListenerPort, actual.Proxy.Expose.Paths[i].ListenerPort)
			assert.Equal(t, expected.Proxy.Expose.Paths[i].LocalPathPort, actual.Proxy.Expose.Paths[i].LocalPathPort)
			assert.Equal(t, expected.Proxy.Expose.Paths[i].ParsedFromCheck, actual.Proxy.Expose.Paths[i].ParsedFromCheck)
			assert.Equal(t, expected.Proxy.Expose.Paths[i].Path, actual.Proxy.Expose.Paths[i].Path)
			assert.Equal(t, expected.Proxy.Expose.Paths[i].Protocol, actual.Proxy.Expose.Paths[i].Protocol)
		}
	}
	assert.Equal(t, expected.Connect.Native, actual.Connect.Native)

	if expected.Connect.SidecarService != nil {
		compareRegistration(t, expected.Connect.SidecarService, actual.Connect.SidecarService)
	}
}

func compareCheck(t *testing.T, expected *AgentServiceCheck, actual *consul.AgentServiceCheck) {
	assert.Equal(t, expected.Args, actual.Args)
	assert.Equal(t, expected.CheckID, actual.CheckID)
	assert.Equal(t, expected.Name, actual.Name)
	assert.Equal(t, expected.DockerContainerID, actual.DockerContainerID)
	assert.Equal(t, expected.Shell, actual.Shell)
	assert.Equal(t, expected.Interval, actual.Interval)
	assert.Equal(t, expected.Timeout, actual.Timeout)
	assert.Equal(t, expected.TTL, actual.TTL)
	assert.Equal(t, expected.HTTP, actual.HTTP)
	assert.Equal(t, expected.Method, actual.Method)
	assert.Equal(t, expected.TCP, actual.TCP)
	assert.Equal(t, expected.Status, actual.Status)
	assert.Equal(t, expected.Notes, actual.Notes)
	assert.Equal(t, expected.GRPC, actual.GRPC)
	assert.Equal(t, expected.AliasNode, actual.AliasNode)
	assert.Equal(t, expected.AliasService, actual.AliasService)
	assert.Equal(t, expected.DeregisterCriticalServiceAfter, actual.DeregisterCriticalServiceAfter)
	assert.Equal(t, expected.Header, actual.Header)
	assert.Equal(t, expected.TLSSkipVerify, actual.TLSSkipVerify)
	assert.Equal(t, expected.GRPCUseTLS, actual.GRPCUseTLS)
}

func getTestPropsWithoutKey(removeKey string) map[string]string {
	metadata := map[string]string{
		nr.AppID:        "test-app",
		nr.AppPort:      "8650",
		nr.DaprPort:     "50001",
		nr.DaprHTTPPort: "3500",
		nr.HostAddress:  "127.0.0.1",
	}
	delete(metadata, removeKey)

	return metadata
}
