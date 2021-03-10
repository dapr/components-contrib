package consul

import (
	"fmt"
	"strconv"
	"testing"

	nr "github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/dapr/pkg/logger"
	consul "github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockConsulResolver struct {
	mock.Mock
}

func (c *mockConsulResolver) InitClient(config *consul.Config) error {
	args := c.Called(config)

	return args.Error(0)
}

func (c *mockConsulResolver) RegisterService(registration *consul.AgentServiceRegistration) error {
	args := c.Called(registration)

	return args.Error(0)
}

func (c *mockConsulResolver) CheckAgent() error {
	args := c.Called()

	return args.Error(0)
}

func (c *mockConsulResolver) GetHealthyServices(serviceID string, queryOptions *consul.QueryOptions) ([]*consul.ServiceEntry, error) {
	args := c.Called(serviceID, queryOptions)

	return args.Get(0).([]*consul.ServiceEntry), args.Error(1)
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
				mockResolver := &mockConsulResolver{}
				mockResolver.On("InitClient", mock.Anything).Return(nil)
				mockResolver.On("RegisterService", mock.Anything).Return(nil)
				mockResolver.On("CheckAgent", mock.Anything).Return(nil)

				_ = newConsulResolver(logger.NewLogger("test"), mockResolver, resolverConfig{}).Init(metadata)

				mockResolver.AssertNumberOfCalls(t, "InitClient", 1)
				mockResolver.AssertNumberOfCalls(t, "RegisterService", 0)
				mockResolver.AssertNumberOfCalls(t, "CheckAgent", 1)
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
				mockResolver := &mockConsulResolver{}
				mockResolver.On("InitClient", mock.Anything).Return(nil)
				mockResolver.On("RegisterService", mock.Anything).Return(nil)
				mockResolver.On("CheckAgent", mock.Anything).Return(nil)

				_ = newConsulResolver(logger.NewLogger("test"), mockResolver, resolverConfig{}).Init(metadata)

				mockResolver.AssertNumberOfCalls(t, "InitClient", 1)
				mockResolver.AssertNumberOfCalls(t, "RegisterService", 1)
				mockResolver.AssertNumberOfCalls(t, "CheckAgent", 0)
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
				mockResolver := &mockConsulResolver{}
				mockResolver.On("InitClient", mock.Anything).Return(nil)
				mockResolver.On("RegisterService", mock.Anything).Return(nil)
				mockResolver.On("CheckAgent", mock.Anything).Return(nil)

				_ = newConsulResolver(logger.NewLogger("test"), mockResolver, resolverConfig{}).Init(metadata)

				mockResolver.AssertNumberOfCalls(t, "InitClient", 1)
				mockResolver.AssertNumberOfCalls(t, "RegisterService", 1)
				mockResolver.AssertNumberOfCalls(t, "CheckAgent", 0)
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
				mockResolver := &mockConsulResolver{}
				mockResolver.On("GetHealthyServices", req.ID, mock.Anything).Return([]*consul.ServiceEntry{}, nil)

				_, err := newConsulResolver(logger.NewLogger("test"), mockResolver, *testConfig).ResolveID(req)
				mockResolver.AssertNumberOfCalls(t, "GetHealthyServices", 1)
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
				mockResolver := &mockConsulResolver{}
				mockResolver.On("GetHealthyServices", req.ID, mock.Anything).Return(
					[]*consul.ServiceEntry{
						{
							Service: &consul.AgentService{
								Address: "123.234.345.456",
								Port:    8600,
								Meta: map[string]string{
									"DAPR_PORT": "50005",
								},
							},
						},
					}, nil)

				addr, _ := newConsulResolver(logger.NewLogger("test"), mockResolver, *testConfig).ResolveID(req)

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
				mockResolver := &mockConsulResolver{}
				mockResolver.On("GetHealthyServices", req.ID, mock.Anything).Return(
					[]*consul.ServiceEntry{
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
					}, nil)

				addr, _ := newConsulResolver(logger.NewLogger("test"), mockResolver, *testConfig).ResolveID(req)

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
				mockResolver := &mockConsulResolver{}
				mockResolver.On("GetHealthyServices", req.ID, mock.Anything).Return(
					[]*consul.ServiceEntry{
						{
							Node: &consul.Node{},
							Service: &consul.AgentService{
								Port: 8600,
								Meta: map[string]string{
									"DAPR_PORT": "50005",
								},
							},
						},
					}, nil)

				_, err := newConsulResolver(logger.NewLogger("test"), mockResolver, *testConfig).ResolveID(req)

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
				mockResolver := &mockConsulResolver{}
				mockResolver.On("GetHealthyServices", req.ID, mock.Anything).Return(
					[]*consul.ServiceEntry{
						{
							Service: &consul.AgentService{
								Address: "123.234.345.456",
								Port:    8600,
							},
						},
					}, nil)

				_, err := newConsulResolver(logger.NewLogger("test"), mockResolver, *testConfig).ResolveID(req)

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
			"empty configuration should only return ClientConfig, QueryOptions and DaprPortMetaKey",
			nr.Metadata{
				Properties:    getTestPropsWithoutKey(""),
				Configuration: nil,
			},
			func(t *testing.T, metadata nr.Metadata) {
				t.Helper()
				actual, _ := getConfig(metadata)

				// Client
				assert.Equal(t, consul.DefaultConfig().Address, actual.ClientConfig.Address)

				// Registration
				assert.Nil(t, actual.Registration)

				// QueryOptions
				assert.NotNil(t, actual.QueryOptions)
				assert.Equal(t, "Checks.ServiceTags contains dapr", actual.QueryOptions.Filter)
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
				assert.Equal(t, consul.DefaultConfig().Address, actual.ClientConfig.Address)

				// Checks
				assert.Equal(t, 1, len(actual.Registration.Checks))
				check := actual.Registration.Checks[0]
				assert.Equal(t, "Dapr Health Status", check.Name)
				assert.Equal(t, "daprHealth:test-app", check.CheckID)
				assert.Equal(t, "15s", check.Interval)
				assert.Equal(t, fmt.Sprintf("http://%s:%s/v1.0/healthz", metadata.Properties[nr.HostAddress], metadata.Properties[nr.DaprHTTPPort]), check.HTTP)

				// Tags
				assert.Equal(t, 1, len(actual.Registration.Tags))
				assert.Equal(t, "dapr", actual.Registration.Tags[0])

				// Metadata
				assert.Equal(t, 1, len(actual.Registration.Meta))
				assert.Equal(t, "50001", actual.Registration.Meta["DAPR_PORT"])

				// QueryOptions
				assert.Equal(t, "Checks.ServiceTags contains dapr", actual.QueryOptions.Filter)
				assert.Equal(t, true, actual.QueryOptions.UseCache)

				// DaprPortMetaKey
				assert.Equal(t, "DAPR_PORT", actual.DaprPortMetaKey)
			},
		},
		{
			"tags without queryOptions should generate filter",
			nr.Metadata{
				Properties: getTestPropsWithoutKey(""),
				Configuration: configSpec{
					Tags: []string{"dapr-A", "dapr-B", "dapr-C"},
				},
			},
			func(t *testing.T, metadata nr.Metadata) {
				t.Helper()
				actual, _ := getConfig(metadata)
				// QueryOptions
				filter := "Checks.ServiceTags contains dapr-A and " +
					"Checks.ServiceTags contains dapr-B and " +
					"Checks.ServiceTags contains dapr-C"
				assert.Equal(t, filter, actual.QueryOptions.Filter)
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
						Port:    000,
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
				assert.Equal(t, 000, actual.Registration.Port)
				assert.Equal(t, "random health check name", actual.Registration.Checks[0].Name)
				assert.Equal(t, "000", actual.Registration.Meta["APP_PORT"])
				assert.Equal(t, "random-tag", actual.Registration.Tags[0])
				assert.Equal(t, "Checks.ServiceTags contains random-tag", actual.QueryOptions.Filter)
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
