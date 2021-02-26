package consul

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"

	consul "github.com/hashicorp/consul/api"

	nr "github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/dapr/pkg/logger"
)

/*
configSpec doc
- blank configuration will not register and will filter consul for given tags and resolve port from given DaprPortMetaKey
- SelfRegister true will register to consul with given tags and given DaprPortMetaKey
- Tags will default to ["dapr"] if not configured, used for registration (if enabled) and resolution filtering
- DaprPortMetaKey will default to "DAPR_PORT", used to 1. set the DaprPort in metadata during registration 2. get the DaprPort from metadata during resolution
- Metadata is for any additional metadata to add if/when registering service
- Checks used for health checks if/when registering, if nil will default to sidecar health check only
- ClientConfig used for connecting to agent, if nil will use sdk defaults
- QueryOptions used for healthy service resolution, if nil will default to useCache and filters for configured tags
- AdvancedRegistration gives full control of registration. Forces configuration of QueryOptions (no default) and
  ignores Checks, Tags, Metadata, SelfRegister
*/
type configSpec struct {
	ClientConfig         *consul.Config
	Checks               []*consul.AgentServiceCheck
	Tags                 []string
	Metadata             map[string]string
	QueryOptions         *consul.QueryOptions
	AdvancedRegistration *consul.AgentServiceRegistration // advanced use-case
	SelfRegister         bool
	DaprPortMetaKey      string
}

const daprTag string = "dapr"       // default tag for register and filter
const daprMeta string = "DAPR_PORT" // default key for DAPR_PORT metadata

var config resolverConfig

type resolverConfig struct {
	ClientConfig    *consul.Config
	QueryOptions    *consul.QueryOptions
	Registration    *consul.AgentServiceRegistration
	DaprPortMetaKey string
}

type resolver struct {
	consul consulResolverInterface
	logger logger.Logger
}

// NewResolver creates Consul name resolver.
func NewResolver(logger logger.Logger) nr.Resolver {
	return newConsulResolver(logger, &consulResolver{})
}

func newConsulResolver(logger logger.Logger, consul consulResolverInterface) nr.Resolver {
	return &resolver{
		logger: logger,
		consul: consul,
	}
}

type consulResolverInterface interface {
	InitClient(config *consul.Config) error
	CheckAgent() error
	RegisterService(registration *consul.AgentServiceRegistration) error
	GetHealthyServices(serviceID string, queryOptions *consul.QueryOptions) ([]*consul.ServiceEntry, error)
	GetConfig() *resolverConfig
}

type consulResolver struct {
	client *consul.Client
}

func (c *consulResolver) InitClient(config *consul.Config) error {
	err := *new(error)
	c.client, err = consul.NewClient(config)
	if err != nil {
		return err
	}
	return nil
}

func (c *consulResolver) RegisterService(registration *consul.AgentServiceRegistration) error {
	return c.client.Agent().ServiceRegister(registration)
}

func (c *consulResolver) CheckAgent() error {
	_, err := c.client.Agent().Self()
	return err
}

func (c *consulResolver) GetConfig() *resolverConfig {
	return &config
}

func (c *consulResolver) GetHealthyServices(serviceID string, queryOptions *consul.QueryOptions) ([]*consul.ServiceEntry, error) {
	services, _, err := c.client.Health().Service(serviceID, "", true, config.QueryOptions)
	return services, err
}

// Init will configure component. It will also register service or validate client connection based on config
func (c *resolver) Init(metadata nr.Metadata) error {
	err := *new(error)

	config, err = getConfig(metadata)
	if err != nil {
		return err
	}

	if err = c.consul.InitClient(config.ClientConfig); err != nil {
		return err
	}

	// register service to consul
	if config.Registration != nil {
		if err := c.consul.RegisterService(config.Registration); err != nil {
			return err
		}

		c.logger.Info("service registered on consul agent")
	} else {
		if err := c.consul.CheckAgent(); err != nil {
			return err
		}
	}

	return nil
}

// ResolveID resolves name to address via consul
func (c *resolver) ResolveID(req nr.ResolveRequest) (string, error) {

	cfg := c.consul.GetConfig()
	services, err := c.consul.GetHealthyServices(req.ID, cfg.QueryOptions)

	if err != nil {
		return "", err
	}

	if len(services) == 0 {
		return "", fmt.Errorf("no healthy services found with AppID:%s", req.ID)
	}

	shuffle := func(services []*consul.ServiceEntry) []*consul.ServiceEntry {
		for i := len(services) - 1; i > 0; i-- {
			j := rand.Int31n(int32(i + 1))
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
		return "", fmt.Errorf("target service AppID:%s found but DAPR_PORT missing from meta ", req.ID)
	}

	return addr, err
}

// getConfig configuration from metadata, defaults are best suited for self-hosted mode
func getConfig(metadata nr.Metadata) (resolverConfig, error) {
	var appID string
	var appPort string
	var host string
	var daprPort string
	var httpPort string
	var ok bool

	resolverConfig := resolverConfig{}

	props := metadata.Properties

	if daprPort, ok = props[nr.DaprPort]; !ok {
		return resolverConfig, fmt.Errorf("nr metadata property missing: %s", nr.DaprPort)
	}

	cfg, err := parseConfig(metadata.Configuration)
	if err != nil {
		return resolverConfig, err
	}

	// If no client config use library defaults
	if cfg.ClientConfig != nil {
		resolverConfig.ClientConfig = cfg.ClientConfig
	} else {
		resolverConfig.ClientConfig = consul.DefaultConfig()
	}

	// if advanced registration configured ignore other registration related configs
	if cfg.AdvancedRegistration != nil {
		if cfg.QueryOptions == nil {
			return resolverConfig, fmt.Errorf("QueryOptions nil - this must be configured if advanced registration")
		}

		resolverConfig.QueryOptions = cfg.QueryOptions
		resolverConfig.Registration = cfg.AdvancedRegistration
	} else {

		// if no tags defined then set default tag for filter
		if len(cfg.Tags) == 0 {
			cfg.Tags = []string{daprTag}
		}

		if cfg.SelfRegister {
			if appID, ok = props[nr.AppID]; !ok {
				return resolverConfig, fmt.Errorf("nr metadata property missing: %s", nr.AppID)
			}

			if appPort, ok = props[nr.AppPort]; !ok {
				return resolverConfig, fmt.Errorf("nr metadata property missing: %s", nr.AppPort)
			}

			if host, ok = props[nr.HostAddress]; !ok {
				return resolverConfig, fmt.Errorf("nr metadata property missing: %s", nr.HostAddress)
			}

			if httpPort, ok = props[nr.DaprHTTPPort]; !ok {
				return resolverConfig, fmt.Errorf("nr metadata property missing: %s", nr.DaprHTTPPort)
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
				return resolverConfig, err
			}

			resolverConfig.Registration = &consul.AgentServiceRegistration{
				Name:    appID,
				Address: host,
				Port:    appPortInt,
				Checks:  cfg.Checks,
				Tags:    cfg.Tags,
				Meta:    cfg.Metadata,
			}
		}

		// if no query options configured add default filter matching every tags in config
		if cfg.QueryOptions == nil {
			filter := ""

			for i, tag := range cfg.Tags {
				if i != 0 {
					filter = filter + " and "
				}
				filter = filter + fmt.Sprintf("Checks.ServiceTags contains %s", tag)
			}

			resolverConfig.QueryOptions = &consul.QueryOptions{
				Filter:   filter,
				UseCache: true,
			}
		}
	}

	// set DaprPortMetaKey used for registring DaprPort and resolving from Consul
	if cfg.DaprPortMetaKey == "" {
		resolverConfig.DaprPortMetaKey = daprMeta
	} else {
		resolverConfig.DaprPortMetaKey = cfg.DaprPortMetaKey
	}

	// if registering, set DaprPort in meta, needed for resolution
	if resolverConfig.Registration != nil {
		if resolverConfig.Registration.Meta == nil {
			resolverConfig.Registration.Meta = map[string]string{}
		}

		resolverConfig.Registration.Meta[resolverConfig.DaprPortMetaKey] = daprPort
	}

	return resolverConfig, nil
}

func parseConfig(rawConfig interface{}) (configSpec, error) {
	config := configSpec{}
	rawConfig = convertGenericConfig(rawConfig)

	data, err := json.Marshal(rawConfig)
	if err != nil {
		return config, err
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&config); err != nil {
		return config, err
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
