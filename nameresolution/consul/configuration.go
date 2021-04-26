package consul

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	consul "github.com/hashicorp/consul/api"
)

// The intermediateConfig is based off of the consul api types. User configurations are
// deserialized into this type before being converted to the equivalent consul types
// that way breaking changes in future versions of the consul api cannot break user configuration
type intermediateConfig struct {
	Client               *Config
	Checks               []*AgentServiceCheck
	Tags                 []string
	Meta                 map[string]string
	QueryOptions         *QueryOptions
	AdvancedRegistration *AgentServiceRegistration // advanced use-case
	SelfRegister         bool
	DaprPortMetaKey      string
}

type configSpec struct {
	Client               *consul.Config
	Checks               []*consul.AgentServiceCheck
	Tags                 []string
	Meta                 map[string]string
	QueryOptions         *consul.QueryOptions
	AdvancedRegistration *consul.AgentServiceRegistration // advanced use-case
	SelfRegister         bool
	DaprPortMetaKey      string
}

func parseConfig(rawConfig interface{}) (configSpec, error) {
	result := configSpec{}
	config := intermediateConfig{}
	rawConfig, err := convertGenericConfig(rawConfig)
	if err != nil {
		return result, err
	}

	data, err := json.Marshal(rawConfig)
	if err != nil {
		return result, fmt.Errorf("error serializing to json: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&config); err != nil {
		return result, fmt.Errorf("error deserializing to configSpec: %w", err)
	}

	result = mapConfig(config)

	return result, nil
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

func mapConfig(config intermediateConfig) configSpec {
	return configSpec{
		Client:               mapClientConfig(config.Client),
		Checks:               mapChecks(config.Checks),
		Tags:                 config.Tags,
		Meta:                 config.Meta,
		QueryOptions:         mapQueryOptions(config.QueryOptions),
		AdvancedRegistration: mapAdvancedRegistration(config.AdvancedRegistration),
		SelfRegister:         config.SelfRegister,
		DaprPortMetaKey:      config.DaprPortMetaKey,
	}
}

func mapClientConfig(config *Config) *consul.Config {
	if config == nil {
		return nil
	}

	mapBasicAuth := func(config *HttpBasicAuth) *consul.HttpBasicAuth {
		if config == nil {
			return nil
		}

		return &consul.HttpBasicAuth{
			Username: config.Username,
			Password: config.Password,
		}
	}

	return &consul.Config{
		Address:    config.Address,
		Scheme:     config.Scheme,
		Datacenter: config.Datacenter,
		HttpAuth:   mapBasicAuth(config.HttpAuth),
		WaitTime:   config.WaitTime,
		Token:      config.Token,
		TokenFile:  config.TokenFile,
		TLSConfig: consul.TLSConfig{
			Address:            config.TLSConfig.Address,
			CAFile:             config.TLSConfig.CAFile,
			CAPath:             config.TLSConfig.CAPath,
			CertFile:           config.TLSConfig.CertFile,
			KeyFile:            config.TLSConfig.KeyFile,
			InsecureSkipVerify: config.TLSConfig.InsecureSkipVerify,
		},
	}
}

func mapChecks(config []*AgentServiceCheck) []*consul.AgentServiceCheck {
	if config == nil {
		return nil
	}

	mapped := []*consul.AgentServiceCheck{}

	for i := 0; i < len(config); i++ {
		mapped = append(mapped, mapCheck(config[i]))
	}

	return mapped
}

func mapCheck(config *AgentServiceCheck) *consul.AgentServiceCheck {
	if config == nil {
		return nil
	}

	return &consul.AgentServiceCheck{
		CheckID:                        config.CheckID,
		Name:                           config.Name,
		Args:                           config.Args,
		DockerContainerID:              config.DockerContainerID,
		Shell:                          config.Shell,
		Interval:                       config.Interval,
		Timeout:                        config.Timeout,
		TTL:                            config.TTL,
		HTTP:                           config.HTTP,
		Header:                         config.Header,
		Method:                         config.Method,
		TCP:                            config.TCP,
		Status:                         config.Status,
		Notes:                          config.Notes,
		TLSSkipVerify:                  config.TLSSkipVerify,
		GRPC:                           config.GRPC,
		GRPCUseTLS:                     config.GRPCUseTLS,
		AliasNode:                      config.AliasNode,
		AliasService:                   config.AliasService,
		DeregisterCriticalServiceAfter: config.DeregisterCriticalServiceAfter,
	}
}

func mapQueryOptions(config *QueryOptions) *consul.QueryOptions {
	if config == nil {
		return nil
	}

	return &consul.QueryOptions{
		Datacenter:        config.Datacenter,
		AllowStale:        config.AllowStale,
		RequireConsistent: config.RequireConsistent,
		UseCache:          config.UseCache,
		MaxAge:            config.MaxAge,
		StaleIfError:      config.StaleIfError,
		WaitIndex:         config.WaitIndex,
		WaitHash:          config.WaitHash,
		WaitTime:          config.WaitTime,
		Token:             config.Token,
		Near:              config.Near,
		NodeMeta:          config.NodeMeta,
		RelayFactor:       config.RelayFactor,
		LocalOnly:         config.LocalOnly,
		Connect:           config.Connect,
		Filter:            config.Filter,
	}
}

func mapAdvancedRegistration(config *AgentServiceRegistration) *consul.AgentServiceRegistration {
	if config == nil {
		return nil
	}

	mapExposeConfig := func(config ExposeConfig) consul.ExposeConfig {
		mapped := consul.ExposeConfig{}

		mapped.Checks = config.Checks

		for i := 0; i < len(config.Paths); i++ {
			tmp := consul.ExposePath{
				ListenerPort:    config.Paths[i].ListenerPort,
				Path:            config.Paths[i].Path,
				LocalPathPort:   config.Paths[i].LocalPathPort,
				Protocol:        config.Paths[i].Protocol,
				ParsedFromCheck: config.Paths[i].ParsedFromCheck,
			}
			mapped.Paths = append(mapped.Paths, tmp)
		}

		return mapped
	}

	mapUpstreams := func(config []Upstream) []consul.Upstream {
		if config == nil {
			return nil
		}

		mapped := []consul.Upstream{}

		for i := 0; i < len(config); i++ {
			tmp := consul.Upstream{
				DestinationType:      consul.UpstreamDestType(config[i].DestinationType),
				DestinationNamespace: config[i].DestinationNamespace,
				DestinationName:      config[i].DestinationName,
				Datacenter:           config[i].Datacenter,
				LocalBindAddress:     config[i].LocalBindAddress,
				LocalBindPort:        config[i].LocalBindPort,
				Config:               config[i].Config,
				MeshGateway:          consul.MeshGatewayConfig{Mode: consul.MeshGatewayMode(config[i].MeshGateway.Mode)},
			}
			mapped = append(mapped, tmp)
		}

		return mapped
	}

	mapProxy := func(config *AgentServiceConnectProxyConfig) *consul.AgentServiceConnectProxyConfig {
		if config == nil {
			return nil
		}

		return &consul.AgentServiceConnectProxyConfig{
			DestinationServiceName: config.DestinationServiceName,
			DestinationServiceID:   config.DestinationServiceID,
			LocalServiceAddress:    config.LocalServiceAddress,
			LocalServicePort:       config.LocalServicePort,
			Config:                 config.Config,
			Upstreams:              mapUpstreams(config.Upstreams),
			MeshGateway:            consul.MeshGatewayConfig{Mode: consul.MeshGatewayMode(config.MeshGateway.Mode)},
			Expose:                 mapExposeConfig(config.Expose),
		}
	}

	mapAgentServiceChecks := func(config AgentServiceChecks) consul.AgentServiceChecks {
		if config == nil {
			return nil
		}

		mapped := consul.AgentServiceChecks{}

		for i := 0; i < len(config); i++ {
			mapped = append(mapped, mapCheck(config[i]))
		}

		return mapped
	}

	mapTaggedAddresses := func(config map[string]ServiceAddress) map[string]consul.ServiceAddress {
		if config == nil {
			return nil
		}

		mapped := map[string]consul.ServiceAddress{}
		for k, v := range config {
			mapped[k] = consul.ServiceAddress{
				Address: v.Address,
				Port:    v.Port,
			}
		}

		return mapped
	}

	mapConnect := func(config *AgentServiceConnect) *consul.AgentServiceConnect {
		if config == nil {
			return nil
		}

		return &consul.AgentServiceConnect{
			Native:         config.Native,
			SidecarService: mapAdvancedRegistration(config.SidecarService),
		}
	}

	mapAgentWeights := func(config *AgentWeights) *consul.AgentWeights {
		if config == nil {
			return nil
		}

		return &consul.AgentWeights{
			Passing: config.Passing,
			Warning: config.Warning,
		}
	}

	mapped := &consul.AgentServiceRegistration{
		Kind:              consul.ServiceKind(config.Kind),
		ID:                config.ID,
		Name:              config.Name,
		Tags:              config.Tags,
		Port:              config.Port,
		Address:           config.Address,
		TaggedAddresses:   mapTaggedAddresses(config.TaggedAddresses),
		EnableTagOverride: config.EnableTagOverride,
		Meta:              config.Meta,
		Weights:           mapAgentWeights(config.Weights),
		Check:             mapCheck(config.Check),
		Checks:            mapAgentServiceChecks(config.Checks),
		Proxy:             mapProxy(config.Proxy),
		Connect:           mapConnect(config.Connect),
	}

	return mapped
}

//nolint:golint,stylecheck
type HttpBasicAuth struct {
	Username string
	Password string
}

type Config struct {
	Address    string
	Scheme     string
	Datacenter string
	HttpAuth   *HttpBasicAuth //nolint:golint,stylecheck
	WaitTime   time.Duration
	Token      string
	TokenFile  string
	TLSConfig  TLSConfig
}

type TLSConfig struct {
	Address            string
	CAFile             string
	CAPath             string
	CertFile           string
	KeyFile            string
	InsecureSkipVerify bool
}

type AgentServiceCheck struct {
	Args                           []string
	CheckID                        string
	Name                           string
	DockerContainerID              string
	Shell                          string
	Interval                       string
	Timeout                        string
	TTL                            string
	HTTP                           string
	Method                         string
	TCP                            string
	Status                         string
	Notes                          string
	GRPC                           string
	AliasNode                      string
	AliasService                   string
	DeregisterCriticalServiceAfter string
	Header                         map[string][]string
	TLSSkipVerify                  bool
	GRPCUseTLS                     bool
}

type QueryOptions struct {
	Datacenter        string
	WaitHash          string
	Token             string
	Near              string
	Filter            string
	MaxAge            time.Duration
	StaleIfError      time.Duration
	WaitIndex         uint64
	WaitTime          time.Duration
	NodeMeta          map[string]string
	AllowStale        bool
	RequireConsistent bool
	UseCache          bool
	RelayFactor       uint8
	LocalOnly         bool
	Connect           bool
}

type AgentServiceRegistration struct {
	Kind              string // original: type ServiceKind string
	ID                string
	Name              string
	Tags              []string
	Port              int
	Address           string
	TaggedAddresses   map[string]ServiceAddress
	EnableTagOverride bool
	Meta              map[string]string
	Weights           *AgentWeights
	Check             *AgentServiceCheck
	Checks            AgentServiceChecks
	Proxy             *AgentServiceConnectProxyConfig
	Connect           *AgentServiceConnect
}

type AgentServiceChecks []*AgentServiceCheck

type ServiceAddress struct {
	Address string
	Port    int
}

type AgentWeights struct {
	Passing int
	Warning int
}

type AgentServiceConnectProxyConfig struct {
	DestinationServiceName string
	DestinationServiceID   string
	LocalServiceAddress    string
	LocalServicePort       int
	Config                 map[string]interface{}
	Upstreams              []Upstream
	MeshGateway            MeshGatewayConfig
	Expose                 ExposeConfig
}

type AgentServiceConnect struct {
	Native         bool
	SidecarService *AgentServiceRegistration
}

type ExposeConfig struct {
	Checks bool
	Paths  []ExposePath
}

type ExposePath struct {
	ListenerPort    int
	Path            string
	LocalPathPort   int
	Protocol        string
	ParsedFromCheck bool
}

type MeshGatewayMode string

type MeshGatewayConfig struct {
	Mode MeshGatewayMode
}

type UpstreamDestType string

type Upstream struct {
	DestinationType      UpstreamDestType
	DestinationNamespace string
	DestinationName      string
	Datacenter           string
	LocalBindAddress     string
	LocalBindPort        int
	Config               map[string]interface{}
	MeshGateway          MeshGatewayConfig
}
