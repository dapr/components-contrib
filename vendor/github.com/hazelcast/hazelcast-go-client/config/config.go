// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package config contains all the configuration to start a Hazelcast instance.
package config

import (
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	defaultGroupName     = "dev"
	defaultGroupPassword = "dev-pass"
)

type Properties map[string]string

// Config is the main configuration to setup a Hazelcast client.
type Config struct {
	// membershipListeners is the array of cluster membership listeners.
	membershipListeners []interface{}

	// lifecycleListeners is the array of listeners for listening to lifecycle events of the Hazelcast instance.
	lifecycleListeners []interface{}

	// groupConfig is the configuration for Hazelcast groups.
	groupConfig *GroupConfig

	// networkConfig is the network configuration of the client.
	networkConfig *NetworkConfig

	// serializationConfig is the serialization configuration of the client.
	serializationConfig *serialization.Config

	// loggerConfig is the configuration for logging.
	loggerConfig *LoggerConfig

	securityConfig *SecurityConfig

	// flakeIDGeneratorConfigMap is mapping of names to flakeIDGeneratorConfigs.
	flakeIDGeneratorConfigMap map[string]*FlakeIDGeneratorConfig

	properties Properties

	// loadBalancer is used to distribute the operations to multiple endpoints.
	loadBalancer core.LoadBalancer

	// clientName is the name of client with this config.
	clientName string

	// reliableTopicConfigMap is mapping of names to ReliableTopicConfigs
	reliableTopicConfigMap map[string]*ReliableTopicConfig
}

// New returns a new Config with default configuration.
func New() *Config {
	return &Config{groupConfig: NewGroupConfig(),
		networkConfig:             NewNetworkConfig(),
		membershipListeners:       make([]interface{}, 0),
		serializationConfig:       serialization.NewConfig(),
		lifecycleListeners:        make([]interface{}, 0),
		flakeIDGeneratorConfigMap: make(map[string]*FlakeIDGeneratorConfig),
		properties:                make(Properties),
		securityConfig:            new(SecurityConfig),
		reliableTopicConfigMap:    make(map[string]*ReliableTopicConfig),
		loggerConfig:              NewLoggerConfig(),
	}
}

// LoggerConfig returns loggerConfig.
func (c *Config) LoggerConfig() *LoggerConfig {
	return c.loggerConfig
}

// SecurityConfig returns the security config for this client.
func (c *Config) SecurityConfig() *SecurityConfig {
	return c.securityConfig
}

// GetReliableTopicConfig returns the reliable topic config for this client.
func (c *Config) GetReliableTopicConfig(name string) *ReliableTopicConfig {
	if cfg, found := c.reliableTopicConfigMap[name]; found {
		return cfg
	}
	_, found := c.reliableTopicConfigMap["default"]
	if !found {
		defConfig := NewReliableTopicConfig("default")
		c.reliableTopicConfigMap["default"] = defConfig
	}
	config := NewReliableTopicConfig(name)
	c.reliableTopicConfigMap[name] = config
	return config

}

// SetSecurityConfig sets the security config for this client.
func (c *Config) SetSecurityConfig(securityConfig *SecurityConfig) {
	c.securityConfig = securityConfig
}

// MembershipListeners returns membership listeners.
func (c *Config) MembershipListeners() []interface{} {
	return c.membershipListeners
}

// AddReliableTopicConfig adds the given reliable topic config to reliable topic configurations.
func (c *Config) AddReliableTopicConfig(config *ReliableTopicConfig) {
	c.reliableTopicConfigMap[config.Name()] = config
}

// LifecycleListeners returns lifecycle listeners.
func (c *Config) LifecycleListeners() []interface{} {
	return c.lifecycleListeners
}

// GroupConfig returns GroupConfig.
func (c *Config) GroupConfig() *GroupConfig {
	return c.groupConfig
}

// NetworkConfig returns NetworkConfig.
func (c *Config) NetworkConfig() *NetworkConfig {
	return c.networkConfig
}

// SerializationConfig returns SerializationConfig.
func (c *Config) SerializationConfig() *serialization.Config {
	return c.serializationConfig
}

// SetProperty sets a new pair of property as (name, value).
func (c *Config) SetProperty(name string, value string) {
	c.properties[name] = value
}

// Properties returns the properties of the config.
func (c *Config) Properties() Properties {
	return c.properties
}

// SetClientName sets the client name.
func (c *Config) SetClientName(name string) {
	c.clientName = name
}

// ClientName returns the client name with this config.
func (c *Config) ClientName() string {
	return c.clientName
}

// LoadBalancer returns loadBalancer for this client.
// If it is not set, this will return nil.
func (c *Config) LoadBalancer() core.LoadBalancer {
	return c.loadBalancer
}

// SetLoadBalancer sets loadBalancer as the given one.
func (c *Config) SetLoadBalancer(loadBalancer core.LoadBalancer) {
	c.loadBalancer = loadBalancer
}

// GetFlakeIDGeneratorConfig returns the FlakeIDGeneratorConfig for the given name, creating one
// if necessary and adding it to the map of known configurations.
// If no configuration is found with the given name it will create a new one with the default Config.
func (c *Config) GetFlakeIDGeneratorConfig(name string) *FlakeIDGeneratorConfig {
	//TODO:: add Config pattern matcher
	if config, found := c.flakeIDGeneratorConfigMap[name]; found {
		return config
	}
	_, found := c.flakeIDGeneratorConfigMap["default"]
	if !found {
		defConfig := NewFlakeIDGeneratorConfig("default")
		c.flakeIDGeneratorConfigMap["default"] = defConfig
	}
	config := NewFlakeIDGeneratorConfig(name)
	c.flakeIDGeneratorConfigMap[name] = config
	return config

}

// AddFlakeIDGeneratorConfig adds the given config to the configurations map.
func (c *Config) AddFlakeIDGeneratorConfig(config *FlakeIDGeneratorConfig) {
	c.flakeIDGeneratorConfigMap[config.Name()] = config
}

// AddMembershipListener adds a membership listener.
func (c *Config) AddMembershipListener(listener interface{}) {
	c.membershipListeners = append(c.membershipListeners, listener)
}

// AddLifecycleListener adds a lifecycle listener.
func (c *Config) AddLifecycleListener(listener interface{}) {
	c.lifecycleListeners = append(c.lifecycleListeners, listener)
}

// SetGroupConfig sets the GroupConfig.
func (c *Config) SetGroupConfig(groupConfig *GroupConfig) {
	c.groupConfig = groupConfig
}

// SetNetworkConfig sets the NetworkConfig.
func (c *Config) SetNetworkConfig(networkConfig *NetworkConfig) {
	c.networkConfig = networkConfig
}

// SetSerializationConfig sets the serialization config.
func (c *Config) SetSerializationConfig(serializationConfig *serialization.Config) {
	c.serializationConfig = serializationConfig
}
