// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dapr/components-contrib/configuration"
	"github.com/dapr/components-contrib/configuration/redis/internal"
	redis "github.com/go-redis/redis/v7"
	jsoniter "github.com/json-iterator/go"

	"github.com/dapr/dapr/pkg/logger"
)

const (
	connectedSlavesReplicas  = "connected_slaves:"
	infoReplicationDelimiter = "\r\n"
	host                     = "redisHost"
	password                 = "redisPassword"
	enableTLS                = "enableTLS"
	maxRetries               = "maxRetries"
	maxRetryBackoff          = "maxRetryBackoff"
	failover                 = "failover"
	sentinelMasterName       = "sentinelMasterName"
	defaultBase              = 10
	defaultBitSize           = 0
	defaultDB                = 0
	defaultMaxRetries        = 3
	defaultMaxRetryBackoff   = time.Second * 2
	defaultEnableTLS         = false
)

// ConfigurationStore is a Redis configuration store
type ConfigurationStore struct {
	client   *redis.Client
	json     jsoniter.API
	metadata metadata
	replicas int

	logger logger.Logger
}

// NewRedisConfigurationStore returns a new redis state store
func NewRedisConfigurationStore(logger logger.Logger) *ConfigurationStore {
	s := &ConfigurationStore{
		json:   jsoniter.ConfigFastest,
		logger: logger,
	}

	return s
}

func parseRedisMetadata(meta configuration.Metadata) (metadata, error) {
	m := metadata{}

	if val, ok := meta.Properties[host]; ok && val != "" {
		m.host = val
	} else {
		return m, errors.New("redis store error: missing host address")
	}

	if val, ok := meta.Properties[password]; ok && val != "" {
		m.password = val
	}

	m.enableTLS = defaultEnableTLS
	if val, ok := meta.Properties[enableTLS]; ok && val != "" {
		tls, err := strconv.ParseBool(val)
		if err != nil {
			return m, fmt.Errorf("redis store error: can't parse enableTLS field: %s", err)
		}
		m.enableTLS = tls
	}

	m.maxRetries = defaultMaxRetries
	if val, ok := meta.Properties[maxRetries]; ok && val != "" {
		parsedVal, err := strconv.ParseInt(val, defaultBase, defaultBitSize)
		if err != nil {
			return m, fmt.Errorf("redis store error: can't parse maxRetries field: %s", err)
		}
		m.maxRetries = int(parsedVal)
	}

	m.maxRetryBackoff = defaultMaxRetryBackoff
	if val, ok := meta.Properties[maxRetryBackoff]; ok && val != "" {
		parsedVal, err := strconv.ParseInt(val, defaultBase, defaultBitSize)
		if err != nil {
			return m, fmt.Errorf("redis store error: can't parse maxRetryBackoff field: %s", err)
		}
		m.maxRetryBackoff = time.Duration(parsedVal)
	}

	if val, ok := meta.Properties[failover]; ok && val != "" {
		failover, err := strconv.ParseBool(val)
		if err != nil {
			return m, fmt.Errorf("redis store error: can't parse failover field: %s", err)
		}
		m.failover = failover
	}

	// set the sentinelMasterName only with failover == true.
	if m.failover {
		if val, ok := meta.Properties[sentinelMasterName]; ok && val != "" {
			m.sentinelMasterName = val
		} else {
			return m, errors.New("redis store error: missing sentinelMasterName")
		}
	}

	return m, nil
}

// Init does metadata and connection parsing
func (r *ConfigurationStore) Init(metadata configuration.Metadata) error {
	m, err := parseRedisMetadata(metadata)
	if err != nil {
		return err
	}
	r.metadata = m

	if r.metadata.failover {
		r.client = r.newFailoverClient(m)
	} else {
		r.client = r.newClient(m)
	}

	if _, err = r.client.Ping().Result(); err != nil {
		return fmt.Errorf("redis store: error connecting to redis at %s: %s", m.host, err)
	}

	r.replicas, err = r.getConnectedSlaves()

	return err
}

func (r *ConfigurationStore) newClient(m metadata) *redis.Client {
	opts := &redis.Options{
		Addr:            m.host,
		Password:        m.password,
		DB:              defaultDB,
		MaxRetries:      m.maxRetries,
		MaxRetryBackoff: m.maxRetryBackoff,
	}

	// tell the linter to skip a check here.
	/* #nosec */
	if m.enableTLS {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: m.enableTLS,
		}
	}

	return redis.NewClient(opts)
}

func (r *ConfigurationStore) newFailoverClient(m metadata) *redis.Client {
	opts := &redis.FailoverOptions{
		MasterName:      r.metadata.sentinelMasterName,
		SentinelAddrs:   []string{r.metadata.host},
		DB:              defaultDB,
		MaxRetries:      m.maxRetries,
		MaxRetryBackoff: m.maxRetryBackoff,
	}

	/* #nosec */
	if m.enableTLS {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: m.enableTLS,
		}
	}

	return redis.NewFailoverClient(opts)
}

func (r *ConfigurationStore) getConnectedSlaves() (int, error) {
	res, err := r.client.DoContext(context.Background(), "INFO", "replication").Result()
	if err != nil {
		return 0, err
	}

	// Response example: https://redis.io/commands/info#return-value
	// # Replication\r\nrole:master\r\nconnected_slaves:1\r\n
	s, _ := strconv.Unquote(fmt.Sprintf("%q", res))
	if len(s) == 0 {
		return 0, nil
	}

	return r.parseConnectedSlaves(s), nil
}

func (r *ConfigurationStore) parseConnectedSlaves(res string) int {
	infos := strings.Split(res, infoReplicationDelimiter)
	for _, info := range infos {
		if strings.Contains(info, connectedSlavesReplicas) {
			parsedReplicas, _ := strconv.ParseUint(info[len(connectedSlavesReplicas):], 10, 32)

			return int(parsedReplicas)
		}
	}

	return 0
}

func (r *ConfigurationStore) Get(ctx context.Context, req *configuration.GetRequest, handler func(e *configuration.UpdateEvent) error) (*configuration.GetResponse, error) {
	if len(req.Keys) == 0 {
		return &configuration.GetResponse{}, nil
	}

	items := make([]*configuration.Item, 0, len(req.Keys))

	// query by keys
	for _, key := range req.Keys {
		redisKey, err := internal.BuildRedisKey(req.AppID, req.Group, req.Label, key)
		if err != nil {
			return &configuration.GetResponse{}, fmt.Errorf("fail to build redis key for key=%s: %s", key, err)
		}
		redisValueMap, err := r.client.HGetAll(redisKey).Result()
		if err != nil {
			return &configuration.GetResponse{}, fmt.Errorf("fail to get configuration for key=%s, redis key=%s: %s", key, redisKey, err)
		}
		if len(redisValueMap) == 0 {
			// Hash key not exist
			continue
		}

		item := &configuration.Item{
			Key:      key,
			Group:    req.Group,
			Label:    req.Label,
			Metadata: map[string]string{},
			Tags:     map[string]string{},
		}
		internal.ParseRedisValue(redisValueMap, item)
		if item.Content != "" {
			// Hash key/value exist and "Content" filed in Hash value exist
			items = append(items, item)
		}
	}
	if req.SubscribeUpdate && handler != nil {
		go r.startSubscribeUpdate(req, handler)
	}

	return &configuration.GetResponse{
		Items: items,
	}, nil
}

func (r *ConfigurationStore) startSubscribeUpdate(req *configuration.GetRequest, handler func(e *configuration.UpdateEvent) error) {
	redisKeys := make([]string, 0, len(req.Keys))
	for _, key := range req.Keys {
		redisKey, _ := internal.BuildRedisKey(req.AppID, req.Group, req.Label, key)
		redisKeys = append(redisKeys, fmt.Sprintf("__key*__:%s", redisKey))
	}

	// enable notify-keyspace-events by redis Set command
	r.client.ConfigSet("notify-keyspace-events", "Kghxe")
	p := r.client.PSubscribe(redisKeys...)

	for msg := range p.Channel() {
		redisKey, err := internal.ParseRedisKeyFromEvent(msg.Channel)
		if err != nil {
			continue
		}

		var item = &configuration.Item{}
		err = internal.ParseRedisKey(redisKey, item)
		if err != nil {
			continue
		}

		switch msg.Payload {
		case "hset":
			redisValueMap, _ := r.client.HGetAll(redisKey).Result()
			internal.ParseRedisValue(redisValueMap, item)
		case "del":
			// left item.Content to empty
		}

		e := &configuration.UpdateEvent{
			AppID: req.AppID,
			Items: []*configuration.Item{
				item,
			},
		}

		err = handler(e)
		if err != nil {
			r.logger.Errorf("fail to call handler to notify event for configuration update subscribe: %s", err)
		}
	}

}

func (r *ConfigurationStore) Save(ctx context.Context, req *configuration.SaveRequest) error {
	if len(req.Items) == 0 {
		return nil
	}

	// 1. prepare redis keys/values to delete or save
	deleteRedisKeys := make([]string, 0, len(req.Items))
	saveRedisKeyValues := make(map[string]map[string]string)

	for _, item := range req.Items {
		if item.Content == "" {
			// Content is empty: delete the configuration item
			redisKey, err := internal.BuildRedisKey(req.AppID, item.Group, item.Label, item.Key)
			if err != nil {
				return fmt.Errorf("fail to build redis key for key=%s: %s", item.Key, err)
			}
			deleteRedisKeys = append(deleteRedisKeys, redisKey)
		} else {
			// save configuration item
			redisKey, err := internal.BuildRedisKey(req.AppID, item.Group, item.Label, item.Key)
			if err != nil {
				return fmt.Errorf("fail to build redis key for key=%s: %s", item.Key, err)
			}
			redisValue, err := internal.BuildRedisValue(item.Content, item.Tags)
			if err != nil {
				return fmt.Errorf("fail to build redis value for key=%s: %s", item.Key, err)
			}
			saveRedisKeyValues[redisKey] = redisValue
		}
	}

	// 2. execute all the delete in a transaction
	pipe := r.client.TxPipeline()
	//for _, redisKey := range deleteRedisKeys {
	//	pipe.Do("DEL", redisKey)
	//}
	if len(deleteRedisKeys) > 0 {
		pipe.Del(deleteRedisKeys...)
	}
	for saveRedisKey, saveRedisValue := range saveRedisKeyValues {
		valueSlides := make([]interface{}, len(saveRedisKey)*2)
		for k, v := range saveRedisValue {
			valueSlides = append(valueSlides, k)
			valueSlides = append(valueSlides, v)
		}
		pipe.HMSet(saveRedisKey, valueSlides...)
	}
	_, err := pipe.Exec()
	if err != nil {
		return fmt.Errorf("fail to save configuration into redis: %s", err)
	}

	return nil
}

func (r *ConfigurationStore) Delete(ctx context.Context, req *configuration.DeleteRequest) error {
	if len(req.Keys) == 0 {
		return nil
	}

	// 1. prepare redis keys to delete
	redisKeys := make([]string, 0, len(req.Keys))
	for _, key := range req.Keys {
		redisKey, err := internal.BuildRedisKey(req.AppID, req.Group, req.Label, key)
		if err != nil {
			return fmt.Errorf("fail to build redis key for key=%s: %s", key, err)
		}
		redisKeys = append(redisKeys, redisKey)
	}

	// 2. execute all the delete in a transaction
	pipe := r.client.TxPipeline()
	pipe.Del(redisKeys...)
	_, err := pipe.Exec()

	if err != nil {
		return fmt.Errorf("fail to delete configuration from redis: %s", err)
	}

	return nil
}
