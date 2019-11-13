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

package internal

import (
	"math"

	"time"

	"strconv"
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/config/property"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/core/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/aggregation"
	"github.com/hazelcast/hazelcast-go-client/internal/discovery"
	"github.com/hazelcast/hazelcast-go-client/internal/predicate"
	"github.com/hazelcast/hazelcast-go-client/internal/projection"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
	"github.com/hazelcast/hazelcast-go-client/internal/reliabletopic"
	"github.com/hazelcast/hazelcast-go-client/security"
	"github.com/hazelcast/hazelcast-go-client/serialization/spi"
)

// clientID is used as a unique id per client. It will be increased atomically when a new client
// is created.
var clientID int64

type HazelcastClient struct {
	InvocationService    invocationService
	Config               *config.Config
	PartitionService     *partitionService
	SerializationService spi.SerializationService
	lifecycleService     *lifecycleService
	ConnectionManager    connectionManager
	ListenerService      *listenerService
	ClusterService       *clusterService
	ProxyManager         *proxyManager
	LoadBalancer         core.LoadBalancer
	HeartBeatService     *heartBeatService
	properties           *property.HazelcastProperties
	credentials          security.Credentials
	name                 string
	id                   int64
	statistics           *statistics
	logger               logger.Logger
}

func NewHazelcastClient(config *config.Config) (*HazelcastClient, error) {
	client := &HazelcastClient{Config: config}
	client.properties = property.NewHazelcastProperties(config.Properties())
	client.id = client.nextClientID()
	client.statistics = newStatistics(client)
	err := client.init()
	return client, err
}

func (c *HazelcastClient) Name() string {
	return c.name
}

func (c *HazelcastClient) GetMap(name string) (core.Map, error) {
	mp, err := c.GetDistributedObject(bufutil.ServiceNameMap, name)
	if err != nil {
		return nil, err
	}
	return mp.(core.Map), nil
}

func (c *HazelcastClient) GetList(name string) (core.List, error) {
	list, err := c.GetDistributedObject(bufutil.ServiceNameList, name)
	if err != nil {
		return nil, err
	}
	return list.(core.List), nil
}

func (c *HazelcastClient) GetSet(name string) (core.Set, error) {
	set, err := c.GetDistributedObject(bufutil.ServiceNameSet, name)
	if err != nil {
		return nil, err
	}
	return set.(core.Set), nil
}

func (c *HazelcastClient) GetReplicatedMap(name string) (core.ReplicatedMap, error) {
	mp, err := c.GetDistributedObject(bufutil.ServiceNameReplicatedMap, name)
	if err != nil {
		return nil, err
	}
	return mp.(core.ReplicatedMap), err
}

func (c *HazelcastClient) GetMultiMap(name string) (core.MultiMap, error) {
	mmp, err := c.GetDistributedObject(bufutil.ServiceNameMultiMap, name)
	if err != nil {
		return nil, err
	}
	return mmp.(core.MultiMap), err
}

func (c *HazelcastClient) GetFlakeIDGenerator(name string) (core.FlakeIDGenerator, error) {
	flakeIDGenerator, err := c.GetDistributedObject(bufutil.ServiceNameIDGenerator, name)
	if err != nil {
		return nil, err
	}
	return flakeIDGenerator.(core.FlakeIDGenerator), err
}

func (c *HazelcastClient) GetTopic(name string) (core.Topic, error) {
	topic, err := c.GetDistributedObject(bufutil.ServiceNameTopic, name)
	if err != nil {
		return nil, err
	}
	return topic.(core.Topic), nil
}

func (c *HazelcastClient) GetReliableTopic(name string) (core.Topic, error) {
	reliableTopic, err := c.GetDistributedObject(bufutil.ServiceNameReliableTopic, name)
	if err != nil {
		return nil, err
	}
	return reliableTopic.(core.Topic), nil
}

func (c *HazelcastClient) GetQueue(name string) (core.Queue, error) {
	queue, err := c.GetDistributedObject(bufutil.ServiceNameQueue, name)
	if err != nil {
		return nil, err
	}
	return queue.(core.Queue), nil
}

func (c *HazelcastClient) GetRingbuffer(name string) (core.Ringbuffer, error) {
	rb, err := c.GetDistributedObject(bufutil.ServiceNameRingbufferService, name)
	if err != nil {
		return nil, err
	}
	return rb.(core.Ringbuffer), nil
}

func (c *HazelcastClient) GetPNCounter(name string) (core.PNCounter, error) {
	counter, err := c.GetDistributedObject(bufutil.ServiceNamePNCounter, name)
	if err != nil {
		return nil, err
	}
	return counter.(core.PNCounter), nil
}

func (c *HazelcastClient) GetDistributedObject(serviceName string, name string) (core.DistributedObject, error) {
	return c.ProxyManager.getOrCreateProxy(serviceName, name)
}

func (c *HazelcastClient) Cluster() core.Cluster {
	return c.ClusterService
}

func (c *HazelcastClient) LifecycleService() core.LifecycleService {
	return c.lifecycleService
}

func (c *HazelcastClient) getLogLevel() int {
	logLevel := c.properties.GetString(property.LoggingLevel)
	logLevelInt, err := logger.GetLogLevel(logLevel)
	if err != nil {
		logLevelInt, _ = logger.GetLogLevel(property.LoggingLevel.DefaultValue())
	}
	return logLevelInt
}

func (c *HazelcastClient) initLogger() {
	setLogger := c.Config.LoggerConfig().Logger()
	logLevel := c.getLogLevel()
	hazelcastPrefix := c.name + " [" + c.Config.GroupConfig().Name() + "]" + " [" + ClientVersion + "] "
	if setLogger == nil {
		l := logger.New()
		l.Level = logLevel
		setLogger = l
	}
	c.logger = newHazelcastLogger(setLogger, hazelcastPrefix)
}

func (c *HazelcastClient) init() error {
	c.initClientName()
	c.initLogger()
	addressTranslator, err := c.createAddressTranslator()
	if err != nil {
		return err
	}
	c.credentials = c.initCredentials(c.Config)
	c.lifecycleService = newLifecycleService(c)
	c.ConnectionManager = newConnectionManager(c, addressTranslator)
	c.HeartBeatService = newHeartBeatService(c)
	c.InvocationService = newInvocationService(c)
	addressProviders := c.createAddressProviders()
	c.ClusterService = newClusterService(c, addressProviders)
	c.ListenerService = newListenerService(c)
	c.PartitionService = newPartitionService(c)
	c.ProxyManager = newProxyManager(c)
	c.LoadBalancer = c.initLoadBalancer(c.Config)
	c.LoadBalancer.Init(c.ClusterService)
	c.initFactories()
	c.SerializationService, err = spi.NewSerializationService(c.Config.SerializationConfig())
	if err != nil {
		return err
	}
	c.PartitionService.start()
	err = c.ClusterService.start()
	if err != nil {
		return err
	}

	c.HeartBeatService.start()
	c.statistics.start()
	c.lifecycleService.fireLifecycleEvent(core.LifecycleStateStarted)
	return nil
}

func (c *HazelcastClient) nextClientID() int64 {
	return atomic.AddInt64(&clientID, 1)
}

func (c *HazelcastClient) initClientName() {
	if c.Config.ClientName() != "" {
		c.name = c.Config.ClientName()
	} else {
		c.name = "hz.client_" + strconv.Itoa(int(c.id))
	}
}
func (c *HazelcastClient) initFactories() {
	c.Config.SerializationConfig().AddDataSerializableFactory(aggregation.FactoryID, aggregation.NewFactory())
	c.Config.SerializationConfig().AddDataSerializableFactory(predicate.FactoryID, predicate.NewFactory())
	c.Config.SerializationConfig().AddDataSerializableFactory(projection.FactoryID, projection.NewFactory())
	c.Config.SerializationConfig().AddDataSerializableFactory(reliabletopic.FactoryID, reliabletopic.NewMessageFactory())
}

func (c *HazelcastClient) initCredentials(cfg *config.Config) security.Credentials {
	groupCfg := cfg.GroupConfig()
	securityCfg := cfg.SecurityConfig()
	creds := securityCfg.Credentials()
	if creds == nil {
		creds = security.NewUsernamePasswordCredentials(groupCfg.Name(), groupCfg.Password())
	}
	return creds
}

func (c *HazelcastClient) createAddressTranslator() (AddressTranslator, error) {
	cloudConfig := c.Config.NetworkConfig().CloudConfig()
	cloudDiscoveryToken := c.properties.GetString(property.HazelcastCloudDiscoveryToken)
	if cloudDiscoveryToken != "" && cloudConfig.IsEnabled() {
		return nil, core.NewHazelcastIllegalStateError("ambigious hazelcast.cloud configuration. "+
			"Both property based and client configuration based settings are provided for Hazelcast "+
			"cloud discovery together. Use only one", nil)
	}
	var hzCloudDiscEnabled bool
	if cloudDiscoveryToken != "" || cloudConfig.IsEnabled() {
		hzCloudDiscEnabled = true
	}
	if hzCloudDiscEnabled {
		var discoveryToken string
		if cloudConfig.IsEnabled() {
			discoveryToken = cloudConfig.DiscoveryToken()
		} else {
			discoveryToken = cloudDiscoveryToken
		}
		urlEndpoint := discovery.CreateURLEndpoint(c.properties, discoveryToken)
		return discovery.NewHzCloudAddrTranslator(urlEndpoint, c.getConnectionTimeout(), c.logger), nil
	}
	return newDefaultAddressTranslator(), nil
}

func (c *HazelcastClient) createAddressProviders() []AddressProvider {
	addressProviders := make([]AddressProvider, 0)
	cloudConfig := c.Config.NetworkConfig().CloudConfig()
	cloudAddressProvider := c.initCloudAddressProvider(cloudConfig)
	if cloudAddressProvider != nil {
		addressProviders = append(addressProviders, cloudAddressProvider)
	}
	addressProviders = append(addressProviders, newDefaultAddressProvider(c.Config.NetworkConfig()))

	return addressProviders
}

func (c *HazelcastClient) initLoadBalancer(config *config.Config) core.LoadBalancer {
	lb := config.LoadBalancer()
	if lb == nil {
		lb = core.NewRoundRobinLoadBalancer()
	}
	return lb
}

func (c *HazelcastClient) initCloudAddressProvider(cloudConfig *config.CloudConfig) *discovery.HzCloudAddrProvider {
	if cloudConfig.IsEnabled() {
		urlEndpoint := discovery.CreateURLEndpoint(c.properties, cloudConfig.DiscoveryToken())
		return discovery.NewHzCloudAddrProvider(urlEndpoint, c.getConnectionTimeout(), c.logger)
	}

	cloudToken := c.properties.GetString(property.HazelcastCloudDiscoveryToken)
	if cloudToken != "" {
		urlEndpoint := discovery.CreateURLEndpoint(c.properties, cloudToken)
		return discovery.NewHzCloudAddrProvider(urlEndpoint, c.getConnectionTimeout(), c.logger)
	}
	return nil
}

func (c *HazelcastClient) getConnectionTimeout() time.Duration {
	nc := c.Config.NetworkConfig()
	connTimeout := nc.ConnectionTimeout()
	if connTimeout == 0 {
		connTimeout = math.MaxInt64
	}
	return connTimeout
}

func (c *HazelcastClient) Shutdown() {
	if c.lifecycleService.isLive.Load().(bool) {
		c.lifecycleService.fireLifecycleEvent(core.LifecycleStateShuttingDown)
		c.ConnectionManager.shutdown()
		c.PartitionService.shutdown()
		c.ClusterService.shutdown()
		c.InvocationService.shutdown()
		c.HeartBeatService.shutdown()
		c.ListenerService.shutdown()
		c.statistics.shutdown()
		c.lifecycleService.fireLifecycleEvent(core.LifecycleStateShutdown)
	}
}
