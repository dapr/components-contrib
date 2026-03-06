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
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	consul "github.com/hashicorp/consul/api"

	nr "github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
)

type client struct {
	*consul.Client
}

func (c *client) InitClient(config *consul.Config) error {
	var err error

	c.Client, err = consul.NewClient(config)
	if err != nil {
		return fmt.Errorf("consul api error initing client: %w", err)
	}

	return nil
}

func (c *client) Agent() agentInterface {
	return c.Client.Agent()
}

func (c *client) Health() healthInterface {
	return c.Client.Health()
}

type clientInterface interface {
	InitClient(config *consul.Config) error
	Agent() agentInterface
	Health() healthInterface
}

type agentInterface interface {
	Self() (map[string]map[string]interface{}, error)
	ServiceRegister(service *consul.AgentServiceRegistration) error
	ServiceDeregister(serviceID string) error
}

type healthInterface interface {
	Service(service, tag string, passingOnly bool, q *consul.QueryOptions) ([]*consul.ServiceEntry, *consul.QueryMeta, error)
	State(state string, q *consul.QueryOptions) (consul.HealthChecks, *consul.QueryMeta, error)
}

type resolver struct {
	config             resolverConfig
	logger             logger.Logger
	client             clientInterface
	registry           registryInterface
	watcherStarted     atomic.Bool
	watcherStopChannel chan struct{}
}

type registryInterface interface {
	getKeys() []string
	get(service string) *registryEntry
	expire(service string) // clears slice of instances
	expireAll()            // clears slice of instances for all entries
	remove(service string) // removes entry from registry
	removeAll()            // removes all entries from registry
	addOrUpdate(service string, services []*consul.ServiceEntry)
	registrationChannel() chan string
}

type registry struct {
	entries        sync.Map
	serviceChannel chan string
}

type registryEntry struct {
	services []*consul.ServiceEntry
	mu       sync.RWMutex
}

func (r *registry) getKeys() []string {
	var keys []string
	r.entries.Range(func(key any, value any) bool {
		k := key.(string)
		keys = append(keys, k)
		return true
	})
	return keys
}

func (r *registry) get(service string) *registryEntry {
	if result, ok := r.entries.Load(service); ok {
		return result.(*registryEntry)
	}

	return nil
}

func (e *registryEntry) next() *consul.ServiceEntry {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.services) == 0 {
		return nil
	}

	// gosec is complaining that we are using a non-crypto-safe PRNG. This is fine in this scenario since we are using it only for selecting a random address for load-balancing.
	//nolint:gosec
	return e.services[rand.Int()%len(e.services)]
}

func (r *resolver) getService(service string) (*consul.ServiceEntry, error) {
	var services []*consul.ServiceEntry

	if r.config.UseCache {
		r.startWatcher()

		entry := r.registry.get(service)
		if entry != nil {
			result := entry.next()

			if result != nil {
				return result, nil
			}
		} else {
			r.registry.registrationChannel() <- service
		}
	}

	options := *r.config.QueryOptions
	options.WaitHash = ""
	options.WaitIndex = 0
	services, _, err := r.client.Health().Service(service, "", true, &options)

	if err != nil {
		return nil, fmt.Errorf("failed to query healthy consul services: %w", err)
	} else if len(services) == 0 {
		return nil, fmt.Errorf("no healthy services found with AppID '%s'", service)
	}

	//nolint:gosec
	return services[rand.Int()%len(services)], nil
}

func (r *registry) addOrUpdate(service string, services []*consul.ServiceEntry) {
	// update
	entry := r.get(service)
	if entry != nil {
		entry.mu.Lock()
		defer entry.mu.Unlock()

		entry.services = services

		return
	}

	// add
	r.entries.Store(service, &registryEntry{
		services: services,
	})
}

func (r *registry) remove(service string) {
	r.entries.Delete(service)
}

func (r *registry) removeAll() {
	r.entries.Range(func(key any, value any) bool {
		r.remove(key.(string))
		return true
	})
}

func (r *registry) expire(service string) {
	entry := r.get(service)
	if entry == nil {
		return
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	entry.services = nil
}

func (r *registry) expireAll() {
	r.entries.Range(func(key any, value any) bool {
		r.expire(key.(string))
		return true
	})
}

func (r *registry) registrationChannel() chan string {
	return r.serviceChannel
}

type resolverConfig struct {
	Client            *consul.Config
	QueryOptions      *consul.QueryOptions
	Registration      *consul.AgentServiceRegistration
	DeregisterOnClose bool
	DaprPortMetaKey   string
	UseCache          bool
}

// NewResolver creates Consul name resolver.
func NewResolver(logger logger.Logger) nr.Resolver {
	return newResolver(logger, resolverConfig{}, &client{}, &registry{serviceChannel: make(chan string, 100)}, make(chan struct{}))
}

func newResolver(logger logger.Logger, resolverConfig resolverConfig, client clientInterface, registry registryInterface, watcherStopChannel chan struct{}) nr.Resolver {
	return &resolver{
		logger:             logger,
		config:             resolverConfig,
		client:             client,
		registry:           registry,
		watcherStopChannel: watcherStopChannel,
	}
}

// Init will configure component. It will also register service or validate client connection based on config.
func (r *resolver) Init(ctx context.Context, metadata nr.Metadata) (err error) {
	r.config, err = getConfig(metadata)
	if err != nil {
		return err
	}

	if r.config.Client.TLSConfig.InsecureSkipVerify {
		r.logger.Infof("hashicorp consul: you are using 'insecureSkipVerify' to skip server config verify which is unsafe!")
	}

	err = r.client.InitClient(r.config.Client)
	if err != nil {
		return fmt.Errorf("failed to init consul client: %w", err)
	}

	// Register service to consul
	if r.config.Registration != nil {
		agent := r.client.Agent()

		err = agent.ServiceRegister(r.config.Registration)
		if err != nil {
			return fmt.Errorf("failed to register consul service: %w", err)
		}

		r.logger.Infof("service:%s registered on consul agent", r.config.Registration.Name)
	} else {
		_, err = r.client.Agent().Self()
		if err != nil {
			return fmt.Errorf("failed check on consul agent: %w", err)
		}
	}

	return nil
}

// ResolveID resolves name to address via consul.
func (r *resolver) ResolveID(ctx context.Context, req nr.ResolveRequest) (addr string, err error) {
	cfg := r.config
	svc, err := r.getService(req.ID)
	if err != nil {
		return "", err
	}

	port := svc.Service.Meta[cfg.DaprPortMetaKey]
	if port == "" {
		return "", fmt.Errorf("target service AppID '%s' found but %s missing from meta", req.ID, cfg.DaprPortMetaKey)
	}

	if svc.Service.Address != "" {
		addr = svc.Service.Address
	} else if svc.Node.Address != "" {
		addr = svc.Node.Address
	} else {
		return "", fmt.Errorf("no healthy services found with AppID '%s'", req.ID)
	}

	return formatAddress(addr, port)
}

// Close will stop the watcher and deregister app from consul
func (r *resolver) Close() error {
	if r.watcherStarted.Load() {
		r.watcherStopChannel <- struct{}{}
	}

	if r.config.Registration != nil && r.config.DeregisterOnClose {
		err := r.client.Agent().ServiceDeregister(r.config.Registration.ID)
		if err != nil {
			return fmt.Errorf("failed to deregister consul service: %w", err)
		}

		r.logger.Info("deregistered service from consul")
	}

	return nil
}

func formatAddress(address string, port string) (addr string, err error) {
	if net.ParseIP(address).To4() != nil {
		return address + ":" + port, nil
	} else if net.ParseIP(address).To16() != nil {
		return fmt.Sprintf("[%s]:%s", address, port), nil
	}

	// addr is not a valid IP address
	// use net.JoinHostPort to format address if address is a valid hostname
	if _, err := net.LookupHost(address); err == nil {
		return net.JoinHostPort(address, port), nil
	}

	return "", fmt.Errorf("invalid ip address or unreachable hostname: %s", address)
}

// getConfig configuration from metadata, defaults are best suited for self-hosted mode.
func getConfig(metadata nr.Metadata) (resolverCfg resolverConfig, err error) {
	props := metadata.GetPropertiesMap()
	if props[nr.DaprPort] == "" {
		return resolverCfg, fmt.Errorf("metadata property missing: %s", nr.DaprPort)
	}

	cfg, err := parseConfig(metadata.Configuration)
	if err != nil {
		return resolverCfg, err
	}

	resolverCfg.DaprPortMetaKey = cfg.DaprPortMetaKey
	resolverCfg.DeregisterOnClose = cfg.SelfDeregister
	resolverCfg.UseCache = cfg.UseCache

	resolverCfg.Client = getClientConfig(cfg)
	resolverCfg.Registration, err = getRegistrationConfig(cfg, props)
	if err != nil {
		return resolverCfg, err
	}
	resolverCfg.QueryOptions = getQueryOptionsConfig(cfg)

	// if registering, set DaprPort in meta, needed for resolution
	if resolverCfg.Registration != nil {
		if resolverCfg.Registration.Meta == nil {
			resolverCfg.Registration.Meta = map[string]string{}
		}

		resolverCfg.Registration.Meta[resolverCfg.DaprPortMetaKey] = props[nr.DaprPort]
	}

	return resolverCfg, nil
}

func getClientConfig(cfg configSpec) *consul.Config {
	// If no client config use library defaults
	if cfg.Client != nil {
		return cfg.Client
	}

	return consul.DefaultConfig()
}

func getRegistrationConfig(cfg configSpec, props map[string]string) (*consul.AgentServiceRegistration, error) {
	// if advanced registration configured ignore other registration related configs
	if cfg.AdvancedRegistration != nil {
		return cfg.AdvancedRegistration, nil
	}
	if !cfg.SelfRegister {
		return nil, nil
	}

	var (
		appID    string
		appPort  string
		host     string
		httpPort string
	)

	appID = props[nr.AppID]
	if appID == "" {
		return nil, fmt.Errorf("metadata property missing: %s", nr.AppID)
	}

	appPort = props[nr.AppPort]
	if appPort == "" {
		return nil, fmt.Errorf("metadata property missing: %s", nr.AppPort)
	}

	host = props[nr.HostAddress]
	if host == "" {
		return nil, fmt.Errorf("metadata property missing: %s", nr.HostAddress)
	}

	httpPort = props[nr.DaprHTTPPort]
	if httpPort == "" {
		return nil, fmt.Errorf("metadata property missing: %s", nr.DaprHTTPPort)
	} else if _, err := strconv.ParseUint(httpPort, 10, 0); err != nil {
		return nil, fmt.Errorf("error parsing %s: %w", nr.DaprHTTPPort, err)
	}

	id := appID + "-" + host + "-" + httpPort
	// if no health checks configured add dapr sidecar health check by default
	if len(cfg.Checks) == 0 {
		cfg.Checks = []*consul.AgentServiceCheck{
			{
				Name:     "Dapr Health Status",
				CheckID:  "daprHealth:" + id,
				Interval: "15s",
				HTTP:     fmt.Sprintf("http://%s/v1.0/healthz?appid=%s", net.JoinHostPort(host, httpPort), appID),
			},
		}
	}

	appPortInt, err := strconv.Atoi(appPort)
	if err != nil {
		return nil, fmt.Errorf("error parsing %s: %w", nr.AppPort, err)
	}

	return &consul.AgentServiceRegistration{
		ID:      id,
		Name:    appID,
		Address: host,
		Port:    appPortInt,
		Checks:  cfg.Checks,
		Tags:    cfg.Tags,
		Meta:    cfg.Meta,
	}, nil
}

func getQueryOptionsConfig(cfg configSpec) *consul.QueryOptions {
	// if no query options configured add default filter matching every tag in config
	if cfg.QueryOptions == nil {
		return &consul.QueryOptions{
			UseCache: true,
		}
	}

	return cfg.QueryOptions
}

func (r *registry) Close() error {
	return nil
}
