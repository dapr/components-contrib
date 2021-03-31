package consul

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
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

const daprMeta string = "DAPR_PORT" // default key for DAPR_PORT metadata

type resolverConfig struct {
	ClientConfig    *consul.Config
	QueryOptions    *consul.QueryOptions
	Registration    *consul.AgentServiceRegistration
	DaprPortMetaKey string
}

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
	config resolverConfig
	logger logger.Logger
	client clientInterface
}

// NewResolver creates Consul name resolver.
func NewResolver(logger logger.Logger) nr.Resolver {
	return newResolver(logger, resolverConfig{}, &client{})
}

func newResolver(logger logger.Logger, resolverConfig resolverConfig, client clientInterface) nr.Resolver {
	return &resolver{
		logger: logger,
		config: resolverConfig,
		client: client,
	}
}

// Init will configure component. It will also register service or validate client connection based on config
func (r *resolver) Init(metadata nr.Metadata) error {
	var err error

	r.config, err = getConfig(metadata)
	if err != nil {
		return err
	}

	if err = r.client.InitClient(r.config.ClientConfig); err != nil {
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
	cfg := r.config
	services, _, err := r.client.Health().Service(req.ID, "", true, cfg.QueryOptions)

	if err != nil {
		return "", fmt.Errorf("failed to query healthy consul services: %w", err)
	}

	if len(services) == 0 {
		return "", fmt.Errorf("no healthy services found with AppID:%s", req.ID)
	}

	shuffle := func(services []*consul.ServiceEntry) []*consul.ServiceEntry {
		for i := len(services) - 1; i > 0; i-- {
			rndbig, _ := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
			j := rndbig.Int64()

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

	return addr, nil
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

	// set DaprPortMetaKey used for registring DaprPort and resolving from Consul
	if cfg.DaprPortMetaKey == "" {
		resolverCfg.DaprPortMetaKey = daprMeta
	} else {
		resolverCfg.DaprPortMetaKey = cfg.DaprPortMetaKey
	}

	resolverCfg.ClientConfig = getClientConfig(cfg)
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

func parseConfig(rawConfig interface{}) (configSpec, error) {
	config := configSpec{}
	rawConfig, err := convertGenericConfig(rawConfig)
	if err != nil {
		return config, err
	}

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
func convertGenericConfig(i interface{}) (interface{}, error) {
	var err error
	switch x := i.(type) {
	case map[interface{}]interface{}:
		m2 := map[string]interface{}{}
		for k, v := range x {
			if strKey, ok := k.(string); ok {
				if m2[strKey], err = convertGenericConfig(v); err != nil {
					return nil, err
				}
			} else {
				return nil, fmt.Errorf("error parsing config field: %v", k)
			}
		}

		return m2, nil
	case []interface{}:
		for i, v := range x {
			if x[i], err = convertGenericConfig(v); err != nil {
				return nil, err
			}
		}
	}

	return i, nil
}
