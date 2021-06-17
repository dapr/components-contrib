package consul

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strconv"
	"sync"

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
}

type healthInterface interface {
	Service(service, tag string, passingOnly bool, q *consul.QueryOptions) ([]*consul.ServiceEntry, *consul.QueryMeta, error)
}

type resolver struct {
	config   resolverConfig
	logger   logger.Logger
	client   clientInterface
	registry registryInterface
}

type registryInterface interface {
	get(service string) *registryEntry
	expire(service string) // clears slice of instances
	remove(service string) // removes entry from registry
	addOrUpdate(service string, services []*consul.ServiceEntry)
}

type registry struct {
	entries map[string]*registryEntry
	mu      sync.RWMutex
}

type registryEntry struct {
	services []*consul.ServiceEntry
	mu       sync.RWMutex
}

func (r *registry) get(service string) *registryEntry {
	return r.entries[service]
}

func (e *registryEntry) next() *consul.ServiceEntry {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if len(e.services) == 0 {
		return nil
	}

	return shuffle(e.services)[0]
}

func (r *resolver) getService(service string) (*consul.ServiceEntry, error) {
	var services []*consul.ServiceEntry

	if r.config.UseCache {
		var entry *registryEntry

		if entry = r.registry.get(service); entry != nil {
			result := entry.next()

			if result != nil {
				return result, nil
			}
		} else {
			r.watchService(service)
		}
	}

	options := *r.config.QueryOptions
	options.WaitHash = ""
	options.WaitIndex = 0
	services, _, err := r.client.Health().Service(service, "", true, &options)

	if err != nil {
		return nil, fmt.Errorf("failed to query healthy consul services: %w", err)
	} else if len(services) == 0 {
		return nil, fmt.Errorf("no healthy services found with AppID:%s", service)
	}

	return shuffle(services)[0], nil
}

func (r *registry) addOrUpdate(service string, services []*consul.ServiceEntry) {
	var entry *registryEntry

	if entry = r.get(service); entry == nil {
		r.mu.Lock()
		if _, ok := r.entries[service]; !ok {
			r.entries[service] = &registryEntry{
				services: services,
			}
		}
		r.mu.Unlock()

		return
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	entry.services = services
}

func (r *registry) remove(service string) {
	if entry := r.get(service); entry == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.entries, service)
}

func (r *registry) expire(service string) {
	var entry *registryEntry

	if entry = r.get(service); entry == nil {
		return
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	entry.services = nil
}

type resolverConfig struct {
	Client          *consul.Config
	QueryOptions    *consul.QueryOptions
	Registration    *consul.AgentServiceRegistration
	DaprPortMetaKey string
	UseCache        bool
}

// NewResolver creates Consul name resolver.
func NewResolver(logger logger.Logger) nr.Resolver {
	return newResolver(logger, resolverConfig{}, &client{}, &registry{entries: map[string]*registryEntry{}})
}

func newResolver(logger logger.Logger, resolverConfig resolverConfig, client clientInterface, registry registryInterface) nr.Resolver {
	return &resolver{
		logger:   logger,
		config:   resolverConfig,
		client:   client,
		registry: registry,
	}
}

// Init will configure component. It will also register service or validate client connection based on config
func (r *resolver) Init(metadata nr.Metadata) error {
	var err error

	r.config, err = getConfig(metadata)
	if err != nil {
		return err
	}

	if err = r.client.InitClient(r.config.Client); err != nil {
		return fmt.Errorf("failed to init consul client: %w", err)
	}

	// register service to consul
	if r.config.Registration != nil {
		if err := r.client.Agent().ServiceRegister(r.config.Registration); err != nil {
			return fmt.Errorf("failed to register consul service: %w", err)
		}

		r.logger.Infof("service:%s registered on consul agent", r.config.Registration.Name)
	} else if _, err := r.client.Agent().Self(); err != nil {
		return fmt.Errorf("failed check on consul agent: %w", err)
	}

	return nil
}

// ResolveID resolves name to address via consul
func (r *resolver) ResolveID(req nr.ResolveRequest) (string, error) {
	var addr string

	svc, err := r.getService(req.ID)

	if err != nil {
		return "", err
	}

	if port, ok := svc.Service.Meta[r.config.DaprPortMetaKey]; ok {
		if svc.Service.Address != "" {
			addr = fmt.Sprintf("%s:%s", svc.Service.Address, port)
		} else if svc.Node.Address != "" {
			addr = fmt.Sprintf("%s:%s", svc.Node.Address, port)
		} else {
			return "", fmt.Errorf("no healthy services found with AppID:%s", req.ID)
		}
	} else {
		return "", fmt.Errorf("target service AppID:%s found but %s missing from meta", req.ID, r.config.DaprPortMetaKey)
	}

	return addr, nil
}

func shuffle(services []*consul.ServiceEntry) []*consul.ServiceEntry {
	for i := len(services) - 1; i > 0; i-- {
		rndbig, _ := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		j := rndbig.Int64()

		services[i], services[j] = services[j], services[i]
	}

	return services
}

// getConfig configuration from metadata, defaults are best suited for self-hosted mode
func getConfig(metadata nr.Metadata) (resolverConfig, error) {
	var daprPort string
	var ok bool
	var err error
	resolverCfg := resolverConfig{}

	props := metadata.Properties

	if daprPort, ok = props[nr.DaprPort]; !ok {
		return resolverCfg, fmt.Errorf("metadata property missing: %s", nr.DaprPort)
	}

	cfg, err := parseConfig(metadata.Configuration)
	if err != nil {
		return resolverCfg, err
	}

	resolverCfg.DaprPortMetaKey = cfg.DaprPortMetaKey
	resolverCfg.UseCache = cfg.UseCache

	resolverCfg.Client = getClientConfig(cfg)
	if resolverCfg.Registration, err = getRegistrationConfig(cfg, props); err != nil {
		return resolverCfg, err
	}
	resolverCfg.QueryOptions = getQueryOptionsConfig(cfg)

	// if registering, set DaprPort in meta, needed for resolution
	if resolverCfg.Registration != nil {
		if resolverCfg.Registration.Meta == nil {
			resolverCfg.Registration.Meta = map[string]string{}
		}

		resolverCfg.Registration.Meta[resolverCfg.DaprPortMetaKey] = daprPort
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
	} else if !cfg.SelfRegister {
		return nil, nil
	}

	var appID string
	var appPort string
	var host string
	var httpPort string
	var ok bool

	if appID, ok = props[nr.AppID]; !ok {
		return nil, fmt.Errorf("metadata property missing: %s", nr.AppID)
	}

	if appPort, ok = props[nr.AppPort]; !ok {
		return nil, fmt.Errorf("metadata property missing: %s", nr.AppPort)
	}

	if host, ok = props[nr.HostAddress]; !ok {
		return nil, fmt.Errorf("metadata property missing: %s", nr.HostAddress)
	}

	if httpPort, ok = props[nr.DaprHTTPPort]; !ok {
		return nil, fmt.Errorf("metadata property missing: %s", nr.DaprHTTPPort)
	} else if _, err := strconv.ParseUint(httpPort, 10, 0); err != nil {
		return nil, fmt.Errorf("error parsing %s: %w", nr.DaprHTTPPort, err)
	}

	// if no health checks configured add dapr sidecar health check by default
	if len(cfg.Checks) == 0 {
		cfg.Checks = []*consul.AgentServiceCheck{
			{
				Name:     "Dapr Health Status",
				CheckID:  fmt.Sprintf("daprHealth:%s", appID),
				Interval: "15s",
				HTTP:     fmt.Sprintf("http://%s:%s/v1.0/healthz", host, httpPort),
			},
		}
	}

	appPortInt, err := strconv.Atoi(appPort)
	if err != nil {
		return nil, fmt.Errorf("error parsing %s: %w", nr.AppPort, err)
	}

	return &consul.AgentServiceRegistration{
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
