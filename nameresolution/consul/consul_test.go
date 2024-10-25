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
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	consul "github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	serviceCalled   int
	serviceErr      *error
	serviceBehavior func(service, tag string, passingOnly bool, q *consul.QueryOptions)
	serviceResult   []*consul.ServiceEntry
	serviceMeta     *consul.QueryMeta

	stateCallStarted atomic.Int32
	stateCalled      int
	stateError       *error
	stateBehaviour   func(state string, q *consul.QueryOptions)
	stateResult      consul.HealthChecks
	stateMeta        *consul.QueryMeta
}

func (m *mockHealth) State(state string, q *consul.QueryOptions) (consul.HealthChecks, *consul.QueryMeta, error) {
	m.stateCallStarted.Add(1)

	if m.stateBehaviour != nil {
		m.stateBehaviour(state, q)
	}

	m.stateCalled++

	if m.stateError == nil {
		return m.stateResult, m.stateMeta, nil
	}

	return m.stateResult, m.stateMeta, *m.stateError
}

func (m *mockHealth) Service(service, tag string, passingOnly bool, q *consul.QueryOptions) ([]*consul.ServiceEntry, *consul.QueryMeta, error) {
	if m.serviceBehavior != nil {
		m.serviceBehavior(service, tag, passingOnly, q)
	}

	m.serviceCalled++

	if m.serviceErr == nil {
		return m.serviceResult, m.serviceMeta, nil
	}

	return m.serviceResult, m.serviceMeta, *m.serviceErr
}

type mockAgent struct {
	selfCalled              int
	selfErr                 error
	selfResult              map[string]map[string]interface{}
	serviceRegisterCalled   int
	serviceRegisterErr      error
	serviceDeregisterCalled int
	serviceDeregisterErr    error
}

func (m *mockAgent) Self() (map[string]map[string]interface{}, error) {
	m.selfCalled++

	return m.selfResult, m.selfErr
}

func (m *mockAgent) ServiceRegister(service *consul.AgentServiceRegistration) error {
	m.serviceRegisterCalled++

	return m.serviceRegisterErr
}

func (m *mockAgent) ServiceDeregister(serviceID string) error {
	m.serviceDeregisterCalled++

	return m.serviceDeregisterErr
}

type mockRegistry struct {
	getKeysCalled         atomic.Int32
	getKeysResult         *[]string
	getKeysBehaviour      func()
	addOrUpdateCalled     atomic.Int32
	addOrUpdateBehaviour  func(service string, services []*consul.ServiceEntry)
	expireCalled          int
	expireAllCalled       int
	removeCalled          int
	removeAllCalled       atomic.Int32
	getCalled             int
	getResult             *registryEntry
	registerChannelResult chan string
}

func (m *mockRegistry) registrationChannel() chan string {
	return m.registerChannelResult
}

func (m *mockRegistry) getKeys() []string {
	if m.getKeysBehaviour != nil {
		m.getKeysBehaviour()
	}

	m.getKeysCalled.Add(1)

	return *m.getKeysResult
}

func (m *mockRegistry) expireAll() {
	m.expireAllCalled++
}

func (m *mockRegistry) removeAll() {
	m.removeAllCalled.Add(1)
}

func (m *mockRegistry) addOrUpdate(service string, services []*consul.ServiceEntry) {
	if m.addOrUpdateBehaviour != nil {
		m.addOrUpdateBehaviour(service, services)
	}

	m.addOrUpdateCalled.Add(1)
}

func (m *mockRegistry) expire(service string) {
	m.expireCalled++
}

func (m *mockRegistry) remove(service string) {
	m.removeCalled++
}

func (m *mockRegistry) get(service string) *registryEntry {
	m.getCalled++

	return m.getResult
}

func TestInit(t *testing.T) {
	tests := []struct {
		testName string
		metadata nr.Metadata
		test     func(*testing.T, nr.Metadata)
	}{
		{
			"given no configuration don't register service just check agent",
			nr.Metadata{Instance: getInstanceInfoWithoutKey(""), Configuration: nil},
			func(t *testing.T, metadata nr.Metadata) {
				var mock mockClient
				resolver := newResolver(logger.NewLogger("test"), resolverConfig{}, &mock, &registry{}, make(chan struct{}))

				_ = resolver.Init(context.Background(), metadata)

				assert.Equal(t, 1, mock.initClientCalled)
				assert.Equal(t, 0, mock.mockAgent.serviceRegisterCalled)
				assert.Equal(t, 1, mock.mockAgent.selfCalled)
			},
		},
		{
			"given SelfRegister true then register service",
			nr.Metadata{
				Instance: getInstanceInfoWithoutKey(""),
				Configuration: configSpec{
					SelfRegister: true,
				},
			},
			func(t *testing.T, metadata nr.Metadata) {
				var mock mockClient
				resolver := newResolver(logger.NewLogger("test"), resolverConfig{}, &mock, &registry{}, make(chan struct{}))

				_ = resolver.Init(context.Background(), metadata)

				assert.Equal(t, 1, mock.initClientCalled)
				assert.Equal(t, 1, mock.mockAgent.serviceRegisterCalled)
				assert.Equal(t, 0, mock.mockAgent.selfCalled)
			},
		},
		{
			"given AdvancedRegistraion then register service",
			nr.Metadata{
				Instance: getInstanceInfoWithoutKey(""),
				Configuration: configSpec{
					AdvancedRegistration: &consul.AgentServiceRegistration{},
					QueryOptions:         &consul.QueryOptions{},
				},
			},
			func(t *testing.T, metadata nr.Metadata) {
				var mock mockClient
				resolver := newResolver(logger.NewLogger("test"), resolverConfig{}, &mock, &registry{}, make(chan struct{}))

				_ = resolver.Init(context.Background(), metadata)

				assert.Equal(t, 1, mock.initClientCalled)
				assert.Equal(t, 1, mock.mockAgent.serviceRegisterCalled)
				assert.Equal(t, 0, mock.mockAgent.selfCalled)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			tt.test(t, tt.metadata)
		})
	}
}

func TestResolveID(t *testing.T) {
	t.Parallel()
	testConfig := resolverConfig{
		DaprPortMetaKey: "DAPR_PORT",
		QueryOptions:    &consul.QueryOptions{},
	}

	tests := []struct {
		testName string
		req      nr.ResolveRequest
		test     func(*testing.T, nr.ResolveRequest)
	}{
		{
			"should use cache when enabled",
			nr.ResolveRequest{
				ID: "test-app",
			},
			func(t *testing.T, req nr.ResolveRequest) {
				blockingCall := make(chan uint64)
				meta := &consul.QueryMeta{
					LastIndex: 0,
				}

				serviceEntries := []*consul.ServiceEntry{
					{
						Service: &consul.AgentService{
							Address: "10.3.245.137",
							Port:    8600,
							Meta: map[string]string{
								"DAPR_PORT": "50005",
							},
						},
					},
				}

				cachedEntries := []*consul.ServiceEntry{
					{
						Service: &consul.AgentService{
							Address: "10.3.245.137",
							Port:    8600,
							Meta: map[string]string{
								"DAPR_PORT": "70007",
							},
						},
					},
				}

				healthChecks := consul.HealthChecks{
					&consul.HealthCheck{
						Node:        "0e1234",
						ServiceID:   "test-app-10.3.245.137-3500",
						ServiceName: "test-app",
						Status:      consul.HealthPassing,
					},
				}

				mock := &mockClient{
					mockHealth: mockHealth{
						// Service()
						serviceResult: serviceEntries,
						serviceMeta:   meta,
						serviceBehavior: func(service, tag string, passingOnly bool, q *consul.QueryOptions) {
						},
						serviceErr: nil,

						// State()
						stateResult: healthChecks,
						stateMeta:   meta,
						stateBehaviour: func(state string, q *consul.QueryOptions) {
							meta.LastIndex = <-blockingCall
						},
						stateError: nil,
					},
				}

				cfg := resolverConfig{
					DaprPortMetaKey: "DAPR_PORT",
					UseCache:        true,
					QueryOptions:    &consul.QueryOptions{},
				}

				serviceKeys := make([]string, 0, 10)

				mockReg := &mockRegistry{
					registerChannelResult: make(chan string, 100),
					getKeysResult:         &serviceKeys,
					addOrUpdateBehaviour: func(service string, services []*consul.ServiceEntry) {
						if services == nil {
							serviceKeys = append(serviceKeys, service)
						}
					},
				}
				resolver := newResolver(logger.NewLogger("test"), cfg, mock, mockReg, make(chan struct{}))
				addr, _ := resolver.ResolveID(context.Background(), req)

				// no apps in registry - cache miss, call agent directly
				assert.Equal(t, 1, mockReg.getCalled)
				waitTillTrueOrTimeout(time.Second, func() bool { return mockReg.getKeysCalled.Load() == 2 })
				assert.Equal(t, 1, mock.mockHealth.serviceCalled)
				assert.Equal(t, "10.3.245.137:50005", addr)

				// watcher adds app to registry
				assert.Equal(t, int32(1), mockReg.addOrUpdateCalled.Load())
				assert.Equal(t, int32(2), mockReg.getKeysCalled.Load())

				mockReg.registerChannelResult <- "test-app"
				mockReg.getResult = &registryEntry{
					services: cachedEntries,
				}

				// blocking query - return new index
				blockingCall <- 2
				waitTillTrueOrTimeout(time.Second, func() bool { return mock.mockHealth.stateCallStarted.Load() == 2 })
				assert.Equal(t, 1, mock.mockHealth.stateCalled)

				// get healthy nodes and update registry for service in result
				assert.Equal(t, 2, mock.mockHealth.serviceCalled)
				assert.Equal(t, int32(2), mockReg.addOrUpdateCalled.Load())

				// resolve id should only hit cache now
				addr, _ = resolver.ResolveID(context.Background(), req)
				assert.Equal(t, "10.3.245.137:70007", addr)
				addr, _ = resolver.ResolveID(context.Background(), req)
				assert.Equal(t, "10.3.245.137:70007", addr)
				addr, _ = resolver.ResolveID(context.Background(), req)
				assert.Equal(t, "10.3.245.137:70007", addr)

				assert.Equal(t, 2, mock.mockHealth.serviceCalled)
				assert.Equal(t, 4, mockReg.getCalled)

				// no update when no change in index and payload
				blockingCall <- 2
				waitTillTrueOrTimeout(time.Second, func() bool { return mock.mockHealth.stateCallStarted.Load() == 3 })
				assert.Equal(t, 2, mock.mockHealth.stateCalled)
				assert.Equal(t, 2, mock.mockHealth.serviceCalled)
				assert.Equal(t, int32(2), mockReg.addOrUpdateCalled.Load())

				// no update when no change in payload
				blockingCall <- 3
				waitTillTrueOrTimeout(time.Second, func() bool { return mock.mockHealth.stateCallStarted.Load() == 4 })
				assert.Equal(t, 3, mock.mockHealth.stateCalled)
				assert.Equal(t, 2, mock.mockHealth.serviceCalled)
				assert.Equal(t, int32(2), mockReg.addOrUpdateCalled.Load())

				// update when change in index and payload
				mock.mockHealth.stateResult[0].Status = consul.HealthCritical
				blockingCall <- 4
				waitTillTrueOrTimeout(time.Second, func() bool { return mock.mockHealth.stateCallStarted.Load() == 5 })
				assert.Equal(t, 4, mock.mockHealth.stateCalled)
				assert.Equal(t, 3, mock.mockHealth.serviceCalled)
				assert.Equal(t, int32(3), mockReg.addOrUpdateCalled.Load())
			},
		},
		{
			"should only update cache on change",
			nr.ResolveRequest{
				ID: "test-app",
			},
			func(t *testing.T, req nr.ResolveRequest) {
				blockingCall := make(chan uint64)
				meta := &consul.QueryMeta{}

				var err error

				// Node 1 all checks healthy
				node1check1 := &consul.HealthCheck{
					Node:        "0e1234",
					ServiceID:   "test-app-10.3.245.137-3500",
					ServiceName: "test-app",
					Status:      consul.HealthPassing,
					CheckID:     "1",
				}

				node1check2 := &consul.HealthCheck{
					Node:        "0e1234",
					ServiceID:   "test-app-10.3.245.137-3500",
					ServiceName: "test-app",
					Status:      consul.HealthPassing,
					CheckID:     "2",
				}

				// Node 2 all checks unhealthy
				node2check1 := &consul.HealthCheck{
					Node:        "0e9878",
					ServiceID:   "test-app-10.3.245.127-3500",
					ServiceName: "test-app",
					Status:      consul.HealthCritical,
					CheckID:     "1",
				}

				node2check2 := &consul.HealthCheck{
					Node:        "0e9878",
					ServiceID:   "test-app-10.3.245.127-3500",
					ServiceName: "test-app",
					Status:      consul.HealthCritical,
					CheckID:     "2",
				}

				mock := mockClient{
					mockHealth: mockHealth{
						// Service()
						serviceResult: []*consul.ServiceEntry{
							{
								Service: &consul.AgentService{
									Address: "10.3.245.137",
									Port:    8600,
									Meta: map[string]string{
										"DAPR_PORT": "50005",
									},
								},
							},
						},
						serviceMeta:     meta,
						serviceBehavior: nil,
						serviceErr:      &err,

						// State()
						stateResult: consul.HealthChecks{
							node1check1,
							node1check2,
							node2check1,
							node2check2,
						},
						stateMeta: meta,
						stateBehaviour: func(state string, q *consul.QueryOptions) {
							meta.LastIndex = <-blockingCall
						},
						stateError: nil,
					},
				}

				cfg := resolverConfig{
					DaprPortMetaKey: "DAPR_PORT",
					UseCache:        true,
					QueryOptions: &consul.QueryOptions{
						WaitIndex: 1,
					},
				}

				serviceKeys := make([]string, 0, 10)

				mockReg := &mockRegistry{
					registerChannelResult: make(chan string, 100),
					getKeysResult:         &serviceKeys,
					addOrUpdateBehaviour: func(service string, services []*consul.ServiceEntry) {
						if services == nil {
							serviceKeys = append(serviceKeys, service)
						}
					},
				}
				resolver := newResolver(logger.NewLogger("test"), cfg, &mock, mockReg, make(chan struct{}))
				addr, _ := resolver.ResolveID(context.Background(), req)

				// no apps in registry - cache miss, call agent directly
				assert.Equal(t, 1, mockReg.getCalled)
				waitTillTrueOrTimeout(time.Second, func() bool { return mockReg.addOrUpdateCalled.Load() == 1 })
				assert.Equal(t, 1, mock.mockHealth.serviceCalled)
				assert.Equal(t, "10.3.245.137:50005", addr)

				// watcher adds app to registry
				assert.Equal(t, int32(1), mockReg.addOrUpdateCalled.Load())
				assert.Equal(t, int32(2), mockReg.getKeysCalled.Load())

				// add key to mock registry - trigger watcher
				mockReg.registerChannelResult <- "test-app"
				mockReg.getResult = &registryEntry{
					services: mock.mockHealth.serviceResult,
				}

				// blocking query - return new index
				blockingCall <- 2
				waitTillTrueOrTimeout(time.Second, func() bool { return mockReg.addOrUpdateCalled.Load() == 2 })
				assert.Equal(t, 1, mock.mockHealth.stateCalled)

				// get healthy nodes and update registry for service in result
				assert.Equal(t, 2, mock.mockHealth.serviceCalled)
				assert.Equal(t, int32(2), mockReg.addOrUpdateCalled.Load())

				// resolve id should only hit cache now
				_, _ = resolver.ResolveID(context.Background(), req)
				_, _ = resolver.ResolveID(context.Background(), req)
				_, _ = resolver.ResolveID(context.Background(), req)
				assert.Equal(t, 2, mock.mockHealth.serviceCalled)

				// change one check for node1 app to critical
				node1check1.Status = consul.HealthCritical

				// blocking query - return new index - node1 app is now unhealthy
				blockingCall <- 3
				waitTillTrueOrTimeout(time.Second, func() bool { return mock.mockHealth.stateCallStarted.Load() == 3 })
				assert.Equal(t, 2, mock.mockHealth.stateCalled)
				assert.Equal(t, 3, mock.mockHealth.serviceCalled)
				assert.Equal(t, int32(3), mockReg.addOrUpdateCalled.Load())

				// change remaining check for node1 app to critical
				node1check2.Status = consul.HealthCritical

				// blocking query - return new index - node1 app is still unhealthy, no change
				blockingCall <- 4
				waitTillTrueOrTimeout(time.Second, func() bool { return mock.mockHealth.stateCallStarted.Load() == 4 })
				assert.Equal(t, 3, mock.mockHealth.stateCalled)
				assert.Equal(t, 3, mock.mockHealth.serviceCalled)
				assert.Equal(t, int32(3), mockReg.addOrUpdateCalled.Load())

				// change one check for node2 app to healthy
				node2check1.Status = consul.HealthPassing

				// blocking query - return new index - node2 app is still unhealthy, no change
				blockingCall <- 4
				waitTillTrueOrTimeout(time.Second, func() bool { return mock.mockHealth.stateCallStarted.Load() == 5 })
				assert.Equal(t, 4, mock.mockHealth.stateCalled)
				assert.Equal(t, 3, mock.mockHealth.serviceCalled)
				assert.Equal(t, int32(3), mockReg.addOrUpdateCalled.Load())

				// change remaining check for node2 app to healthy
				node2check2.Status = consul.HealthPassing

				// blocking query - return new index - node2 app is now healthy
				blockingCall <- 5
				waitTillTrueOrTimeout(time.Second, func() bool { return mock.mockHealth.stateCallStarted.Load() == 6 })
				assert.Equal(t, 5, mock.mockHealth.stateCalled)
				assert.Equal(t, 4, mock.mockHealth.serviceCalled)
				assert.Equal(t, int32(4), mockReg.addOrUpdateCalled.Load())
			},
		},
		{
			"should expire cache upon blocking call error",
			nr.ResolveRequest{
				ID: "test-app",
			},
			func(t *testing.T, req nr.ResolveRequest) {
				blockingCall := make(chan uint64)
				meta := &consul.QueryMeta{
					LastIndex: 0,
				}

				err := errors.New("oh no")

				serviceEntries := []*consul.ServiceEntry{
					{
						Service: &consul.AgentService{
							Address: "10.3.245.137",
							Port:    8600,
							Meta: map[string]string{
								"DAPR_PORT": "50005",
							},
						},
					},
				}

				healthChecks := consul.HealthChecks{
					&consul.HealthCheck{
						Node:        "0e1234",
						ServiceID:   "test-app-10.3.245.137-3500",
						ServiceName: "test-app",
						Status:      consul.HealthPassing,
					},
				}

				mock := &mockClient{
					mockHealth: mockHealth{
						// Service()
						serviceResult: serviceEntries,
						serviceMeta:   meta,
						serviceBehavior: func(service, tag string, passingOnly bool, q *consul.QueryOptions) {
						},
						serviceErr: nil,

						// State()
						stateResult: healthChecks,
						stateMeta:   meta,
						stateBehaviour: func(state string, q *consul.QueryOptions) {
							meta.LastIndex = <-blockingCall
						},
						stateError: nil,
					},
				}

				cfg := resolverConfig{
					DaprPortMetaKey: "DAPR_PORT",
					UseCache:        true,
					QueryOptions:    &consul.QueryOptions{},
				}

				serviceKeys := make([]string, 0, 10)

				mockReg := &mockRegistry{
					registerChannelResult: make(chan string, 100),
					getKeysResult:         &serviceKeys,
					addOrUpdateBehaviour: func(service string, services []*consul.ServiceEntry) {
						if services == nil {
							serviceKeys = append(serviceKeys, service)
						}
					},
				}
				resolver := newResolver(logger.NewLogger("test"), cfg, mock, mockReg, make(chan struct{}))
				addr, _ := resolver.ResolveID(context.Background(), req)

				// Cache miss pass through
				assert.Equal(t, 1, mockReg.getCalled)
				waitTillTrueOrTimeout(time.Second, func() bool { return mockReg.addOrUpdateCalled.Load() == 1 })
				assert.Equal(t, 1, mock.mockHealth.serviceCalled)
				assert.Equal(t, int32(1), mockReg.addOrUpdateCalled.Load())
				assert.Equal(t, "10.3.245.137:50005", addr)

				waitTillTrueOrTimeout(time.Second, func() bool { return mock.mockHealth.stateCallStarted.Load() == 1 })
				mockReg.getKeysResult = &serviceKeys
				mockReg.registerChannelResult <- "test-app"
				mockReg.getResult = &registryEntry{
					services: serviceEntries,
				}

				blockingCall <- 2
				waitTillTrueOrTimeout(time.Second, func() bool { return mockReg.addOrUpdateCalled.Load() == 2 })
				assert.Equal(t, 1, mock.mockHealth.stateCalled)
				assert.Equal(t, 2, mock.mockHealth.serviceCalled)
				assert.Equal(t, int32(2), mockReg.addOrUpdateCalled.Load())

				mock.mockHealth.stateError = &err
				blockingCall <- 3
				blockingCall <- 3
				waitTillTrueOrTimeout(time.Second, func() bool { return mock.mockHealth.stateCallStarted.Load() == 2 })
				assert.Equal(t, 1, mockReg.expireAllCalled)
			},
		},
		{
			"should stop watcher on close",
			nr.ResolveRequest{
				ID: "test-app",
			},
			func(t *testing.T, req nr.ResolveRequest) {
				blockingCall := make(chan uint64)
				meta := &consul.QueryMeta{
					LastIndex: 0,
				}

				serviceEntries := []*consul.ServiceEntry{
					{
						Service: &consul.AgentService{
							Address: "10.3.245.137",
							Port:    8600,
							Meta: map[string]string{
								"DAPR_PORT": "50005",
							},
						},
					},
				}

				healthChecks := consul.HealthChecks{
					&consul.HealthCheck{
						Node:        "0e1234",
						ServiceID:   "test-app-10.3.245.137-3500",
						ServiceName: "test-app",
						Status:      consul.HealthPassing,
					},
				}

				mock := &mockClient{
					mockHealth: mockHealth{
						// Service()
						serviceResult: serviceEntries,
						serviceMeta:   meta,
						serviceBehavior: func(service, tag string, passingOnly bool, q *consul.QueryOptions) {
						},
						serviceErr: nil,

						// State()
						stateResult: healthChecks,
						stateMeta:   meta,
						stateBehaviour: func(state string, q *consul.QueryOptions) {
							select {
							case meta.LastIndex = <-blockingCall:
							case <-q.Context().Done():
							}
						},
						stateError: nil,
					},
				}

				cfg := resolverConfig{
					DaprPortMetaKey: "DAPR_PORT",
					UseCache:        true,
					QueryOptions:    &consul.QueryOptions{},
				}

				serviceKeys := make([]string, 0, 10)

				mockReg := &mockRegistry{
					registerChannelResult: make(chan string, 100),
					getKeysResult:         &serviceKeys,
					addOrUpdateBehaviour: func(service string, services []*consul.ServiceEntry) {
						if services == nil {
							serviceKeys = append(serviceKeys, service)
						}
					},
				}
				resolver := newResolver(logger.NewLogger("test"), cfg, mock, mockReg, make(chan struct{})).(*resolver)
				addr, _ := resolver.ResolveID(context.Background(), req)

				// Cache miss pass through
				assert.Equal(t, 1, mockReg.getCalled)
				waitTillTrueOrTimeout(time.Second, func() bool { return mockReg.addOrUpdateCalled.Load() == 1 })
				assert.Equal(t, 1, mock.mockHealth.serviceCalled)
				assert.Equal(t, int32(1), mockReg.addOrUpdateCalled.Load())
				assert.Equal(t, "10.3.245.137:50005", addr)

				waitTillTrueOrTimeout(time.Second, func() bool { return mock.mockHealth.stateCallStarted.Load() == 1 })
				mockReg.getKeysResult = &serviceKeys
				mockReg.registerChannelResult <- "test-app"
				mockReg.getResult = &registryEntry{
					services: serviceEntries,
				}

				resolver.Close()
				waitTillTrueOrTimeout(time.Second*1, func() bool { return mockReg.removeAllCalled.Load() == 1 })
				assert.Equal(t, int32(1), mockReg.removeAllCalled.Load())
				assert.False(t, resolver.watcherStarted.Load())
			},
		},
		{
			"error if no healthy services found",
			nr.ResolveRequest{
				ID: "test-app",
			},
			func(t *testing.T, req nr.ResolveRequest) {
				mock := mockClient{
					mockHealth: mockHealth{
						serviceResult: []*consul.ServiceEntry{},
					},
				}
				resolver := newResolver(logger.NewLogger("test"), testConfig, &mock, &registry{}, make(chan struct{}))

				_, err := resolver.ResolveID(context.Background(), req)
				assert.Equal(t, 1, mock.mockHealth.serviceCalled)
				require.Error(t, err)
			},
		},
		{
			"should get address from service",
			nr.ResolveRequest{
				ID: "test-app",
			},
			func(t *testing.T, req nr.ResolveRequest) {
				mock := mockClient{
					mockHealth: mockHealth{
						serviceResult: []*consul.ServiceEntry{
							{
								Service: &consul.AgentService{
									Address: "10.3.245.137",
									Port:    8600,
									Meta: map[string]string{
										"DAPR_PORT": "50005",
									},
								},
							},
						},
					},
				}
				resolver := newResolver(logger.NewLogger("test"), testConfig, &mock, &registry{}, make(chan struct{}))

				addr, _ := resolver.ResolveID(context.Background(), req)

				assert.Equal(t, "10.3.245.137:50005", addr)
			},
		},
		{
			"should get ipv6 address from service",
			nr.ResolveRequest{
				ID: "test-app",
			},
			func(t *testing.T, req nr.ResolveRequest) {
				mock := mockClient{
					mockHealth: mockHealth{
						serviceResult: []*consul.ServiceEntry{
							{
								Service: &consul.AgentService{
									Address: "2001:db8:3333:4444:5555:6666:7777:8888",
									Port:    8600,
									Meta: map[string]string{
										"DAPR_PORT": "50005",
									},
								},
							},
						},
					},
				}
				resolver := newResolver(logger.NewLogger("test"), testConfig, &mock, &registry{}, make(chan struct{}))

				addr, _ := resolver.ResolveID(context.Background(), req)

				assert.Equal(t, "[2001:db8:3333:4444:5555:6666:7777:8888]:50005", addr)
			},
		},
		{
			"should get localhost (hostname) from service",
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
									Address: "localhost",
									Port:    8600,
									Meta: map[string]string{
										"DAPR_PORT": "50005",
									},
								},
							},
						},
					},
				}
				resolver := newResolver(logger.NewLogger("test"), testConfig, &mock, &registry{}, make(chan struct{}))

				addr, _ := resolver.ResolveID(context.Background(), req)

				assert.Equal(t, "localhost:50005", addr)
			},
		},
		{
			"should get random address from service",
			nr.ResolveRequest{
				ID: "test-app",
			},
			func(t *testing.T, req nr.ResolveRequest) {
				mock := mockClient{
					mockHealth: mockHealth{
						serviceResult: []*consul.ServiceEntry{
							{
								Service: &consul.AgentService{
									Address: "10.3.245.137",
									Port:    8600,
									Meta: map[string]string{
										"DAPR_PORT": "50005",
									},
								},
							},
							{
								Service: &consul.AgentService{
									Address: "234.245.255.228",
									Port:    8600,
									Meta: map[string]string{
										"DAPR_PORT": "50005",
									},
								},
							},
						},
					},
				}
				resolver := newResolver(logger.NewLogger("test"), testConfig, &mock, &registry{}, make(chan struct{}))

				total1 := 0
				total2 := 0
				for range 100 {
					addr, _ := resolver.ResolveID(context.Background(), req)

					if addr == "10.3.245.137:50005" {
						total1++
					} else if addr == "234.245.255.228:50005" {
						total2++
					} else {
						t.Fatalf("Received unexpected address: %s", addr)
					}
				}

				// Because of the random nature of the address being returned, we just check to make sure we get at least 20 of each (and a total of 100)
				assert.Equal(t, 100, total1+total2)
				assert.Greater(t, total1, 20)
				assert.Greater(t, total2, 20)
			},
		},
		{
			"should get address from node if not on service",
			nr.ResolveRequest{
				ID: "test-app",
			},
			func(t *testing.T, req nr.ResolveRequest) {
				mock := mockClient{
					mockHealth: mockHealth{
						serviceResult: []*consul.ServiceEntry{
							{
								Node: &consul.Node{
									Address: "10.3.245.137",
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
									Address: "10.3.245.137",
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
				resolver := newResolver(logger.NewLogger("test"), testConfig, &mock, &registry{}, make(chan struct{}))

				addr, _ := resolver.ResolveID(context.Background(), req)

				assert.Equal(t, "10.3.245.137:50005", addr)
			},
		},
		{
			"error if no address found on service",
			nr.ResolveRequest{
				ID: "test-app",
			},
			func(t *testing.T, req nr.ResolveRequest) {
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
				resolver := newResolver(logger.NewLogger("test"), testConfig, &mock, &registry{}, make(chan struct{}))

				_, err := resolver.ResolveID(context.Background(), req)

				require.Error(t, err)
			},
		},
		{
			"error if consul service missing DaprPortMetaKey",
			nr.ResolveRequest{
				ID: "test-app",
			},
			func(t *testing.T, req nr.ResolveRequest) {
				mock := mockClient{
					mockHealth: mockHealth{
						serviceResult: []*consul.ServiceEntry{
							{
								Service: &consul.AgentService{
									Address: "123.234.145.155",
									Port:    8600,
								},
							},
						},
					},
				}
				resolver := newResolver(logger.NewLogger("test"), testConfig, &mock, &registry{}, make(chan struct{}))

				_, err := resolver.ResolveID(context.Background(), req)

				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			tt.test(t, tt.req)
		})
	}
}

func TestClose(t *testing.T) {
	tests := []struct {
		testName string
		metadata nr.Metadata
		test     func(*testing.T, nr.Metadata)
	}{
		{
			"should deregister",
			nr.Metadata{Instance: getInstanceInfoWithoutKey(""), Configuration: nil},
			func(t *testing.T, metadata nr.Metadata) {
				var mock mockClient
				cfg := resolverConfig{
					Registration:      &consul.AgentServiceRegistration{},
					DeregisterOnClose: true,
				}

				resolver := newResolver(logger.NewLogger("test"), cfg, &mock, &registry{}, make(chan struct{})).(*resolver)
				resolver.Close()

				assert.Equal(t, 1, mock.mockAgent.serviceDeregisterCalled)
			},
		},
		{
			"should not deregister",
			nr.Metadata{Instance: getInstanceInfoWithoutKey(""), Configuration: nil},
			func(t *testing.T, metadata nr.Metadata) {
				var mock mockClient
				cfg := resolverConfig{
					Registration:      &consul.AgentServiceRegistration{},
					DeregisterOnClose: false,
				}

				resolver := newResolver(logger.NewLogger("test"), cfg, &mock, &registry{}, make(chan struct{})).(*resolver)
				resolver.Close()

				assert.Equal(t, 0, mock.mockAgent.serviceDeregisterCalled)
			},
		},
		{
			"should not deregister when no registration",
			nr.Metadata{Instance: getInstanceInfoWithoutKey(""), Configuration: nil},
			func(t *testing.T, metadata nr.Metadata) {
				var mock mockClient
				cfg := resolverConfig{
					Registration:      nil,
					DeregisterOnClose: true,
				}

				resolver := newResolver(logger.NewLogger("test"), cfg, &mock, &registry{}, make(chan struct{})).(*resolver)
				resolver.Close()

				assert.Equal(t, 0, mock.mockAgent.serviceDeregisterCalled)
			},
		},
		{
			"should stop watcher if started",
			nr.Metadata{Instance: getInstanceInfoWithoutKey(""), Configuration: nil},
			func(t *testing.T, metadata nr.Metadata) {
				var mock mockClient
				resolver := newResolver(logger.NewLogger("test"), resolverConfig{}, &mock, &registry{}, make(chan struct{})).(*resolver)
				resolver.watcherStarted.Store(true)

				go resolver.Close()

				sleepTimer := time.NewTimer(time.Second)
				watcherStoppedInItem := false
				select {
				case <-sleepTimer.C:
				case <-resolver.watcherStopChannel:
					watcherStoppedInItem = true
				}

				assert.True(t, watcherStoppedInItem)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			t.Parallel()
			tt.test(t, tt.metadata)
		})
	}
}

func TestRegistry(t *testing.T) {
	appID := "myService"
	tests := []struct {
		testName string
		test     func(*testing.T)
	}{
		{
			"should add and update entry",
			func(t *testing.T) {
				registry := &registry{}

				result := []*consul.ServiceEntry{
					{
						Service: &consul.AgentService{
							Address: "10.3.245.137",
							Port:    8600,
						},
					},
				}

				registry.addOrUpdate(appID, result)

				entry, _ := registry.entries.Load(appID)
				assert.Equal(t, result, entry.(*registryEntry).services)

				update := []*consul.ServiceEntry{
					{
						Service: &consul.AgentService{
							Address: "random",
							Port:    123,
						},
					},
				}

				registry.addOrUpdate(appID, update)
				entry, _ = registry.entries.Load(appID)
				assert.Equal(t, update, entry.(*registryEntry).services)
			},
		},
		{
			"should expire entries",
			func(t *testing.T) {
				registry := &registry{}
				registry.entries.Store(
					"A",
					&registryEntry{
						services: []*consul.ServiceEntry{
							{
								Service: &consul.AgentService{
									Address: "10.3.245.137",
									Port:    8600,
								},
							},
						},
					})

				registry.entries.Store(
					"B",
					&registryEntry{
						services: []*consul.ServiceEntry{
							{
								Service: &consul.AgentService{
									Address: "10.3.245.137",
									Port:    8600,
								},
							},
						},
					})

				registry.entries.Store(
					"C",
					&registryEntry{
						services: []*consul.ServiceEntry{
							{
								Service: &consul.AgentService{
									Address: "10.3.245.137",
									Port:    8600,
								},
							},
						},
					})

				result, _ := registry.entries.Load("A")
				assert.NotNil(t, result.(*registryEntry).services)

				registry.expire("A")

				result, _ = registry.entries.Load("A")
				assert.Nil(t, result.(*registryEntry).services)

				registry.expireAll()
				count := 0
				nilCount := 0
				registry.entries.Range(func(key, value any) bool {
					count++
					if value.(*registryEntry).services == nil {
						nilCount++
					}
					return true
				})

				assert.Equal(t, 3, count)
				assert.Equal(t, 3, nilCount)
			},
		},
		{
			"should remove entry",
			func(t *testing.T) {
				registry := &registry{}
				entry := &registryEntry{
					services: []*consul.ServiceEntry{
						{
							Service: &consul.AgentService{
								Address: "10.3.245.137",
								Port:    8600,
							},
						},
					},
				}

				registry.entries.Store("A", entry)
				registry.entries.Store("B", entry)
				registry.entries.Store("C", entry)
				registry.entries.Store("D", entry)

				registry.remove("A")

				result, _ := registry.entries.Load("A")
				assert.Nil(t, result)

				result, _ = registry.entries.Load("B")
				assert.NotNil(t, result)

				registry.removeAll()
				count := 0
				registry.entries.Range(func(key, value any) bool {
					count++
					return true
				})

				assert.Equal(t, 0, count)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			t.Parallel()
			tt.test(t)
		})
	}
}

func TestParseConfig(t *testing.T) {
	tests := []struct {
		testName    string
		shouldParse bool
		input       interface{}
		expected    configSpec
	}{
		{
			"valid configuration in metadata",
			true,
			map[any]any{
				"Checks": []interface{}{
					map[any]any{
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
				"Meta": map[any]any{
					"APP_PORT":       "123",
					"DAPR_HTTP_PORT": "3500",
					"DAPR_GRPC_PORT": "50005",
				},
				"QueryOptions": map[any]any{
					"UseCache": true,
					"Filter":   "Checks.ServiceTags contains dapr",
				},
				"DaprPortMetaKey": "DAPR_PORT",
				"UseCache":        false,
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
				DaprPortMetaKey: "DAPR_PORT",
				UseCache:        false,
			},
		},
		{
			"empty configuration in metadata",
			true,
			nil,
			configSpec{
				DaprPortMetaKey: defaultDaprPortMetaKey,
			},
		},
		{
			"fail on unsupported map key",
			false,
			map[any]any{
				1000: map[any]any{
					"DAPR_HTTP_PORT": "3500",
					"DAPR_GRPC_PORT": "50005",
				},
			},
			configSpec{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			actual, err := parseConfig(tt.input)

			if tt.shouldParse {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, actual)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestGetConfig(t *testing.T) {
	tests := []struct {
		testName string
		metadata nr.Metadata
		test     func(*testing.T, nr.Metadata)
	}{
		{
			"empty configuration should only return Client, QueryOptions and DaprPortMetaKey",
			nr.Metadata{
				Instance:      getInstanceInfoWithoutKey(""),
				Configuration: nil,
			},
			func(t *testing.T, metadata nr.Metadata) {
				actual, _ := getConfig(metadata)

				// Client
				assert.Equal(t, consul.DefaultConfig().Address, actual.Client.Address)

				// Registration
				assert.Nil(t, actual.Registration)

				// QueryOptions
				assert.NotNil(t, actual.QueryOptions)
				assert.True(t, actual.QueryOptions.UseCache)

				// DaprPortMetaKey
				assert.Equal(t, defaultDaprPortMetaKey, actual.DaprPortMetaKey)

				// Cache
				assert.False(t, actual.UseCache)
			},
		},
		{
			"empty configuration with SelfRegister should default correctly",
			nr.Metadata{
				Instance: getInstanceInfoWithoutKey(""),
				Configuration: map[any]any{
					"SelfRegister": true,
				},
			},
			func(t *testing.T, metadata nr.Metadata) {
				actual, _ := getConfig(metadata)
				// Client
				assert.Equal(t, consul.DefaultConfig().Address, actual.Client.Address)

				// Checks
				assert.Len(t, actual.Registration.Checks, 1)
				check := actual.Registration.Checks[0]
				assert.Equal(t, "Dapr Health Status", check.Name)
				assert.Equal(t, "daprHealth:test-app-"+metadata.Instance.Address+"-"+strconv.Itoa(metadata.Instance.DaprHTTPPort), check.CheckID)
				assert.Equal(t, "15s", check.Interval)
				assert.Equal(t, fmt.Sprintf("http://%s/v1.0/healthz?appid=%s", net.JoinHostPort(metadata.Instance.Address, strconv.Itoa(metadata.Instance.DaprHTTPPort)), metadata.Instance.AppID), check.HTTP)

				// Metadata
				assert.Len(t, actual.Registration.Meta, 1)
				assert.Equal(t, "50001", actual.Registration.Meta[actual.DaprPortMetaKey])

				// QueryOptions
				assert.True(t, actual.QueryOptions.UseCache)

				// DaprPortMetaKey
				assert.Equal(t, defaultDaprPortMetaKey, actual.DaprPortMetaKey)

				// Cache
				assert.False(t, actual.UseCache)
			},
		},
		{
			"DaprPortMetaKey should set registration meta and config used for resolve",
			nr.Metadata{
				Instance: getInstanceInfoWithoutKey(""),
				Configuration: map[any]any{
					"SelfRegister":    true,
					"DaprPortMetaKey": "random_key",
				},
			},
			func(t *testing.T, metadata nr.Metadata) {
				actual, _ := getConfig(metadata)

				daprPort := strconv.Itoa(metadata.Instance.DaprInternalPort)

				assert.Equal(t, "random_key", actual.DaprPortMetaKey)
				assert.Equal(t, daprPort, actual.Registration.Meta["random_key"])
			},
		},
		{
			"SelfDeregister should set DeregisterOnClose",
			nr.Metadata{
				Instance: getInstanceInfoWithoutKey(""),
				Configuration: map[any]any{
					"SelfRegister":   true,
					"SelfDeregister": true,
				},
			},
			func(t *testing.T, metadata nr.Metadata) {
				actual, _ := getConfig(metadata)

				assert.True(t, actual.DeregisterOnClose)
			},
		},
		{
			"missing AppID property should error when SelfRegister true",
			nr.Metadata{
				Instance: getInstanceInfoWithoutKey("AppID"),
				Configuration: map[any]any{
					"SelfRegister": true,
				},
			},
			func(t *testing.T, metadata nr.Metadata) {
				_, err := getConfig(metadata)
				require.Error(t, err)
				assert.Contains(t, err.Error(), nr.AppID)

				metadata.Configuration = configSpec{
					SelfRegister: false,
				}

				_, err = getConfig(metadata)
				require.NoError(t, err)

				metadata.Configuration = configSpec{
					AdvancedRegistration: &consul.AgentServiceRegistration{},
					QueryOptions:         &consul.QueryOptions{},
				}

				_, err = getConfig(metadata)
				require.NoError(t, err)
			},
		},
		{
			"missing AppPort property should error when SelfRegister true",
			nr.Metadata{
				Instance: getInstanceInfoWithoutKey("AppPort"),
				Configuration: map[any]any{
					"SelfRegister": true,
				},
			},
			func(t *testing.T, metadata nr.Metadata) {
				_, err := getConfig(metadata)
				require.Error(t, err)
				assert.Contains(t, err.Error(), nr.AppPort)

				metadata.Configuration = configSpec{
					SelfRegister: false,
				}

				_, err = getConfig(metadata)
				require.NoError(t, err)

				metadata.Configuration = configSpec{
					AdvancedRegistration: &consul.AgentServiceRegistration{},
					QueryOptions:         &consul.QueryOptions{},
				}

				_, err = getConfig(metadata)
				require.NoError(t, err)
			},
		},
		{
			"missing Address property should error when SelfRegister true",
			nr.Metadata{
				Instance: getInstanceInfoWithoutKey("Address"),
				Configuration: map[any]any{
					"SelfRegister": true,
				},
			},
			func(t *testing.T, metadata nr.Metadata) {
				_, err := getConfig(metadata)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "HOST_ADDRESS")

				metadata.Configuration = configSpec{
					SelfRegister: false,
				}

				_, err = getConfig(metadata)
				require.NoError(t, err)

				metadata.Configuration = configSpec{
					AdvancedRegistration: &consul.AgentServiceRegistration{},
					QueryOptions:         &consul.QueryOptions{},
				}

				_, err = getConfig(metadata)
				require.NoError(t, err)
			},
		},
		{
			"missing DaprHTTPPort property should error only when SelfRegister true",
			nr.Metadata{
				Instance: getInstanceInfoWithoutKey("DaprHTTPPort"),
				Configuration: map[any]any{
					"SelfRegister": true,
				},
			},
			func(t *testing.T, metadata nr.Metadata) {
				_, err := getConfig(metadata)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "DAPR_HTTP_PORT")

				metadata.Configuration = configSpec{
					SelfRegister: false,
				}

				_, err = getConfig(metadata)
				require.NoError(t, err)

				metadata.Configuration = configSpec{
					AdvancedRegistration: &consul.AgentServiceRegistration{},
					QueryOptions:         &consul.QueryOptions{},
				}

				_, err = getConfig(metadata)
				require.NoError(t, err)
			},
		},
		{
			"missing DaprInternalPort property should always error",
			nr.Metadata{
				Instance: getInstanceInfoWithoutKey("DaprInternalPort"),
			},
			func(t *testing.T, metadata nr.Metadata) {
				metadata.Configuration = configSpec{
					SelfRegister: false,
				}

				_, err := getConfig(metadata)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "DAPR_PORT")

				metadata.Configuration = configSpec{
					SelfRegister: true,
				}

				_, err = getConfig(metadata)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "DAPR_PORT")

				metadata.Configuration = configSpec{
					AdvancedRegistration: &consul.AgentServiceRegistration{},
					QueryOptions:         &consul.QueryOptions{},
				}

				_, err = getConfig(metadata)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "DAPR_PORT")
			},
		},
		{
			"registration should configure correctly",
			nr.Metadata{
				Instance: getInstanceInfoWithoutKey(""),
				Configuration: map[any]any{
					"Checks": []interface{}{
						map[any]any{
							"Name":     "test-app health check name",
							"CheckID":  "test-app health check id",
							"Interval": "15s",
							"HTTP":     "http://127.0.0.1:3500/health",
						},
					},
					"Tags": []interface{}{
						"test",
					},
					"Meta": map[any]any{
						"APP_PORT":       "8650",
						"DAPR_GRPC_PORT": "50005",
					},
					"QueryOptions": map[any]any{
						"UseCache": false,
						"Filter":   "Checks.ServiceTags contains something",
					},
					"SelfRegister":    true,
					"DaprPortMetaKey": "PORT",
					"UseCache":        false,
				},
			},
			func(t *testing.T, metadata nr.Metadata) {
				actual, _ := getConfig(metadata)

				// Enabled Registration
				assert.NotNil(t, actual.Registration)
				assert.Equal(t, metadata.Instance.AppID, actual.Registration.Name)
				assert.Equal(t, metadata.Instance.Address, actual.Registration.Address)
				assert.Equal(t, metadata.Instance.AppPort, actual.Registration.Port)
				assert.Equal(t, "test-app health check name", actual.Registration.Checks[0].Name)
				assert.Equal(t, "test-app health check id", actual.Registration.Checks[0].CheckID)
				assert.Equal(t, "15s", actual.Registration.Checks[0].Interval)
				assert.Equal(t, "http://127.0.0.1:3500/health", actual.Registration.Checks[0].HTTP)
				assert.Equal(t, "test", actual.Registration.Tags[0])
				assert.Equal(t, "8650", actual.Registration.Meta["APP_PORT"])
				assert.Equal(t, "50005", actual.Registration.Meta["DAPR_GRPC_PORT"])
				assert.Equal(t, strconv.Itoa(metadata.Instance.DaprInternalPort), actual.Registration.Meta["PORT"])
				assert.False(t, actual.QueryOptions.UseCache)
				assert.Equal(t, "Checks.ServiceTags contains something", actual.QueryOptions.Filter)
				assert.Equal(t, "PORT", actual.DaprPortMetaKey)
				assert.False(t, actual.UseCache)
			},
		},
		{
			"advanced registration should override/ignore other configs",
			nr.Metadata{
				Instance: getInstanceInfoWithoutKey(""),
				Configuration: map[any]any{
					"AdvancedRegistration": map[any]any{
						"Name":    "random-app-id",
						"Port":    0o00,
						"Address": "123.345.678",
						"Tags":    []string{"random-tag"},
						"Meta": map[string]string{
							"APP_PORT": "000",
						},
						"Checks": []interface{}{
							map[any]any{
								"Name":     "random health check name",
								"CheckID":  "random health check id",
								"Interval": "15s",
								"HTTP":     "http://127.0.0.1:3500/health",
							},
						},
					},
					"Checks": []interface{}{
						map[any]any{
							"Name":     "test-app health check name",
							"CheckID":  "test-app health check id",
							"Interval": "15s",
							"HTTP":     "http://127.0.0.1:3500/health",
						},
					},
					"Tags": []string{
						"dapr",
						"test",
					},
					"Meta": map[string]string{
						"APP_PORT":       "123",
						"DAPR_HTTP_PORT": "3500",
						"DAPR_GRPC_PORT": "50005",
					},
					"SelfRegister": false,
				},
			},
			func(t *testing.T, metadata nr.Metadata) {
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
		t.Run(tt.testName, func(t *testing.T) {
			tt.test(t, tt.metadata)
		})
	}
}

func TestMapConfig(t *testing.T) {
	t.Run("should map full configuration", func(t *testing.T) {
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
			UseCache:        false,
		}

		actual := mapConfig(expected)

		compareQueryOptions(t, expected.QueryOptions, actual.QueryOptions)
		compareRegistration(t, expected.AdvancedRegistration, actual.AdvancedRegistration)
		compareClientConfig(t, expected.Client, actual.Client)

		for i := range len(expected.Checks) {
			compareCheck(t, expected.Checks[i], actual.Checks[i])
		}

		assert.Equal(t, expected.Tags, actual.Tags)
		assert.Equal(t, expected.Meta, actual.Meta)
		assert.Equal(t, expected.SelfRegister, actual.SelfRegister)
		assert.Equal(t, expected.DaprPortMetaKey, actual.DaprPortMetaKey)
		assert.Equal(t, expected.UseCache, actual.UseCache)
	})

	t.Run("should map empty configuration", func(t *testing.T) {
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

	for i := range len(expected.Checks) {
		compareCheck(t, expected.Checks[i], actual.Checks[i])
	}

	if expected.Proxy != nil {
		assert.Equal(t, expected.Proxy.DestinationServiceName, actual.Proxy.DestinationServiceName)
		assert.Equal(t, expected.Proxy.DestinationServiceID, actual.Proxy.DestinationServiceID)
		assert.Equal(t, expected.Proxy.LocalServiceAddress, actual.Proxy.LocalServiceAddress)
		assert.Equal(t, expected.Proxy.LocalServicePort, actual.Proxy.LocalServicePort)
		assert.Equal(t, expected.Proxy.Config, actual.Proxy.Config)

		for i := range len(expected.Proxy.Upstreams) {
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

		for i := range len(expected.Proxy.Expose.Paths) {
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

func getInstanceInfoWithoutKey(removeKey string) nr.Instance {
	res := nr.Instance{
		AppID:            "test-app",
		AppPort:          8650,
		DaprInternalPort: 50001,
		DaprHTTPPort:     3500,
		Address:          "127.0.0.1",
	}

	switch removeKey {
	case "AppID":
		res.AppID = ""
	case "AppPort":
		res.AppPort = 0
	case "DaprInternalPort":
		res.DaprInternalPort = 0
	case "DaprHTTPPort":
		res.DaprHTTPPort = 0
	case "Address":
		res.Address = ""
	}

	return res
}

func waitTillTrueOrTimeout(d time.Duration, condition func() bool) {
	for range 100 {
		if condition() {
			return
		}

		time.Sleep(d / 100)
	}
}
