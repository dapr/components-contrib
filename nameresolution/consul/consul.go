package consul

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"strconv"

	consul "github.com/hashicorp/consul/api"

	nr "github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/dapr/pkg/logger"
)

type configSpec struct {
	ClientConfig         *consul.Config
	Checks               []*consul.AgentServiceCheck
	Tags                 []string
	Meta                 map[string]string
	QueryOptions         *consul.QueryOptions
	AdvancedRegistration *consul.AgentServiceRegistration // advanced use-case
	SelfRegister         bool
	DaprPortMetaKey      string
}

const daprTag string = "dapr"       // default tag for register and filter
const daprMeta string = "DAPR_PORT" // default key for DAPR_PORT metadata

type resolverConfig struct {
	ClientConfig    *consul.Config
	QueryOptions    *consul.QueryOptions
	Registration    *consul.AgentServiceRegistration
	DaprPortMetaKey string
}

type resolver struct {
	config resolverConfig
	consul consulResolverInterface
	logger logger.Logger
}

// NewResolver creates Consul name resolver.
func NewResolver(logger logger.Logger) nr.Resolver {
	return newConsulResolver(logger, &consulResolver{}, resolverConfig{})
}

func newConsulResolver(logger logger.Logger, consul consulResolverInterface, config resolverConfig) nr.Resolver {
	return &resolver{
		logger: logger,
		consul: consul,
		config: config,
	}
}

type consulResolverInterface interface {
	InitClient(config *consul.Config) error
	CheckAgent() error
	RegisterService(registration *consul.AgentServiceRegistration) error
	GetHealthyServices(serviceID string, queryOptions *consul.QueryOptions) ([]*consul.ServiceEntry, error)
}

type consulResolver struct {
	client *consul.Client
}

func (c *consulResolver) InitClient(config *consul.Config) error {
	var err error
	c.client, err = consul.NewClient(config)
	if err != nil {
		return fmt.Errorf("consul api error initing client: %w", err)
	}

	return nil
}

func (c *consulResolver) RegisterService(registration *consul.AgentServiceRegistration) error {
	return c.client.Agent().ServiceRegister(registration)
}

func (c *consulResolver) CheckAgent() error {
	_, err := c.client.Agent().Self()

	return fmt.Errorf("consul api error getting agent metadata: %w", err)
}

func (c *consulResolver) GetHealthyServices(serviceID string, queryOptions *consul.QueryOptions) ([]*consul.ServiceEntry, error) {
	services, _, err := c.client.Health().Service(serviceID, "", true, queryOptions)

	return services, fmt.Errorf("consul api error querying health service: %w", err)
}

// Init will configure component. It will also register service or validate client connection based on config
func (c *resolver) Init(metadata nr.Metadata) error {
	var err error

	c.config, err = getConfig(metadata)
	if err != nil {
		return err
	}

	if err = c.consul.InitClient(c.config.ClientConfig); err != nil {
		return fmt.Errorf("failed to init consul client: %w", err)
	}

	// register service to consul
	if c.config.Registration != nil {
		if err := c.consul.RegisterService(c.config.Registration); err != nil {
			return fmt.Errorf("failed to register consul service: %w", err)
		}

		c.logger.Infof("service:%s registered on consul agent", c.config.Registration.Name)
	} else if err := c.consul.CheckAgent(); err != nil {
		return fmt.Errorf("failed check on consul agent: %w", err)
	}

	return nil
}

// ResolveID resolves name to address via consul
func (c *resolver) ResolveID(req nr.ResolveRequest) (string, error) {
	cfg := c.config
	services, err := c.consul.GetHealthyServices(req.ID, cfg.QueryOptions)

	if err != nil {
		return "", fmt.Errorf("failed to query healthy consul services: %w", err)
	}

	if len(services) == 0 {
		return "", fmt.Errorf("no healthy services found with AppID:%s", req.ID)
	}

	shuffle := func(services []*consul.ServiceEntry) []*consul.ServiceEntry {
		for i := len(services) - 1; i > 0; i-- {
			j, _ := rand.Read(make([]byte, i+1))
			services[i], services[j] = services[j], services[i]
		}

		return services
	}

	svc := shuffle(services)[0]

	// TODO - kubernetes behaviour?

	addr := ""

	if port, ok := svc.Service.Meta[cfg.DaprPortMetaKey]; ok {
		if svc.Service.Address != "" {
			addr = fmt.Sprintf("%s:%s", svc.Service.Address, port)
		} else if svc.Node.Address != "" {
			addr = fmt.Sprintf("%s:%s", svc.Node.Address, port)
		} else {
			return "", fmt.Errorf("no healthy services found with AppID:%s", req.ID)
		}
	} else {
		return "", fmt.Errorf("target service AppID:%s found but DAPR_PORT missing from meta", req.ID)
	}

	return addr, fmt.Errorf("error resolving AppID:%s: %w", req.ID, err)
}

// getConfig configuration from metadata, defaults are best suited for self-hosted mode
func getConfig(metadata nr.Metadata) (resolverConfig, error) {
	var daprPort string
	var ok bool
	var err error
	resolverCfg := resolverConfig{}

	props := metadata.Properties

	if daprPort, ok = props[nr.DaprPort]; !ok {
		return resolverCfg, fmt.Errorf("nr metadata property missing: %s", nr.DaprPort)
	}

	cfg, err := parseConfig(metadata.Configuration)
	if err != nil {
		return resolverCfg, err
	}

	// if no tags defined then set default tag for filter
	if len(cfg.Tags) == 0 {
		cfg.Tags = []string{daprTag}
	}

	resolverCfg.ClientConfig = getClientConfig(cfg)
	if resolverCfg.Registration, err = getRegistrationConfig(cfg, props); err != nil {
		return resolverCfg, err
	}
	resolverCfg.QueryOptions = getQueryOptionsConfig(cfg, resolverCfg.Registration)

	// set DaprPortMetaKey used for registring DaprPort and resolving from Consul
	if cfg.DaprPortMetaKey == "" {
		resolverCfg.DaprPortMetaKey = daprMeta
	} else {
		resolverCfg.DaprPortMetaKey = cfg.DaprPortMetaKey
	}

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
	if cfg.ClientConfig != nil {
		return cfg.ClientConfig
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
		return nil, fmt.Errorf("nr metadata property missing: %s", nr.AppID)
	}

	if appPort, ok = props[nr.AppPort]; !ok {
		return nil, fmt.Errorf("nr metadata property missing: %s", nr.AppPort)
	}

	if host, ok = props[nr.HostAddress]; !ok {
		return nil, fmt.Errorf("nr metadata property missing: %s", nr.HostAddress)
	}

	if httpPort, ok = props[nr.DaprHTTPPort]; !ok {
		return nil, fmt.Errorf("nr metadata property missing: %s", nr.DaprHTTPPort)
	}

	// if no health checks configured add dapr sidecar health check by default
	if len(cfg.Checks) == 0 {
		cfg.Checks = []*consul.AgentServiceCheck{
			{
				Name:     "Dapr Health Status",
				CheckID:  fmt.Sprintf("daprHealth:%s", appID),
				Interval: "15s",
				HTTP:     fmt.Sprintf("http://%s:%s/v1.0/healthz", host, httpPort), // default assumes consul agent on svc host
			},
		}
	}

	appPortInt, err := strconv.Atoi(appPort)
	if err != nil {
		return nil, fmt.Errorf("error parsing app port: %w", err)
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

func getQueryOptionsConfig(cfg configSpec, registration *consul.AgentServiceRegistration) *consul.QueryOptions {
	// if no query options configured add default filter matching every tag in config
	if cfg.QueryOptions == nil {
		var filterTags []string
		filter := ""

		if registration != nil {
			filterTags = registration.Tags
		} else {
			filterTags = cfg.Tags
		}

		for i, tag := range filterTags {
			if i != 0 {
				filter += " and "
			}
			filter += fmt.Sprintf("Checks.ServiceTags contains %s", tag)
		}

		return &consul.QueryOptions{
			Filter:   filter,
			UseCache: true,
		}
	}

	return cfg.QueryOptions
}

func parseConfig(rawConfig interface{}) (configSpec, error) {
	config := configSpec{}
	rawConfig = convertGenericConfig(rawConfig)

	data, err := json.Marshal(rawConfig)
	if err != nil {
		return config, fmt.Errorf("error serializing to json: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&config); err != nil {
		return config, fmt.Errorf("error deserializing to configSpec: %w", err)
	}

	return config, nil
}

// helper function for transforming interface{} into json serializable
func convertGenericConfig(i interface{}) interface{} {
	switch x := i.(type) {
	case map[interface{}]interface{}:
		m2 := map[string]interface{}{}
		for k, v := range x {
			m2[k.(string)] = convertGenericConfig(v)
		}

		return m2
	case []interface{}:
		for i, v := range x {
			x[i] = convertGenericConfig(v)
		}
	}

	return i
}
