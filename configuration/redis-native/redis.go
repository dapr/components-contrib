// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis_native

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dapr/components-contrib/configuration"
	"github.com/dapr/components-contrib/configuration/redis-native/internal"
	redis "github.com/go-redis/redis/v7"
	jsoniter "github.com/json-iterator/go"

	"github.com/dapr/kit/logger"
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
func NewRedisConfigurationStore(logger logger.Logger) configuration.Store {
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

func (r *ConfigurationStore) Get(ctx context.Context, req *configuration.GetRequest) (*configuration.GetResponse, error) {
	items := make([]*configuration.Item, 0, 16)

	// step1: get all the keys for this app
	pattern, err := internal.BuildRedisKeyPattern(req.AppID)
	if err != nil {
		return nil, err
	}
	keys, err := r.client.Keys(pattern).Result()
	if err != nil {
		return nil, err
	}

	var revision = ""

	// query by keys
	for _, redisKey := range keys {
		item := &configuration.Item{
			Metadata: map[string]string{},
			Tags:     map[string]string{},
		}

		err := internal.ParseRedisKey(redisKey, item)
		if err != nil {
			// it should not happen, skip it
			continue
		}
		if internal.IsRevisionKey(item.Name) {
			revision, _ = r.client.Get(redisKey).Result()
			continue
		}

		redisValueMap, err := r.client.HGetAll(redisKey).Result()
		if err != nil {
			// TODO: should we return error or just skip this key?
			return &configuration.GetResponse{}, fmt.Errorf("fail to get configuration for key=%s, redis key=%s: %s", item.Name, redisKey, err)
		}
		if len(redisValueMap) == 0 {
			// Hash key not exist: it should not happen, skip it
			continue
		}

		internal.ParseRedisValue(redisValueMap, item)
		if item.Content != "" {
			// Hash key/value exist and "Content" filed in Hash value exist
			items = append(items, item)
		}
	}

	return &configuration.GetResponse{
		AppID: req.AppID,
		Revision: revision,
		Items: items,
	}, nil
}

func (r *ConfigurationStore) Subscribe(ctx context.Context, req *configuration.SubscribeRequest, handler configuration.UpdateHandler) error {
	// not subscribed yet, start to subscribe and save to cache
	redisKey4revision, err := internal.BuildRedisKey4Revision(req.AppID)
	if err != nil {
		return err
	}
	redisChannel := fmt.Sprintf("__keyspace*__:%s", redisKey4revision)
	go r.doSubscribe(ctx, req, handler, redisChannel)

	return nil
}

func (r *ConfigurationStore) doSubscribe(ctx context.Context, req *configuration.SubscribeRequest, handler configuration.UpdateHandler, redisChannel4revision string) error {
	// enable notify-keyspace-events by redis Set command
	// r.client.ConfigSet("notify-keyspace-events", "KA")
	p := r.client.PSubscribe(redisChannel4revision)
	for msg := range p.Channel() {
		r.handleSubscribedChange(ctx, req, handler, msg)
	}

	return nil
}

func (r *ConfigurationStore) handleSubscribedChange(ctx context.Context, req *configuration.SubscribeRequest, handler configuration.UpdateHandler, msg *redis.Message) {
	defer func() {
		if err := recover(); err != nil {
			r.logger.Errorf("panic in handleSubscribedChange(）method and recovered: %s", err)
		}
	}()

	_, err := internal.ParseRedisKeyFromEvent(msg.Channel)
	if err != nil {
		return
	}

	getResponse, err := r.Get(ctx, &configuration.GetRequest {
		AppID: req.AppID,
		Metadata: req.Metadata,
	})
	if err != nil {
		return
	}

	e := &configuration.UpdateEvent{
		AppID: req.AppID,
		Revision: getResponse.Revision,
		Items: getResponse.Items,
	}
	err = handler(ctx, e)
	if err != nil {
		r.logger.Errorf("fail to call handler to notify event for configuration update subscribe: %s", err)
	}
}

func (r *ConfigurationStore) Delete(ctx context.Context, req *configuration.DeleteRequest) error {
	// 1. prepare redis keys to delete
	pattern, err := internal.BuildRedisKeyPattern(req.AppID)
	if err != nil {
		return err
	}
	redisKeys, err := r.client.Keys(pattern).Result()
	if err != nil {
		return err
	}

	if len(redisKeys) == 0 {
		return nil
	}

	revisionKey, _ := internal.BuildRedisKey4Revision(req.AppID)
	deleteKeys := make([]string, 0, len(redisKeys))
	for _, key := range redisKeys {
		if key != revisionKey {
			deleteKeys = append(deleteKeys, key)
		}
	}

	// 2. execute all the delete in a transaction
	if len(deleteKeys) > 0 {
		pipe := r.client.TxPipeline()
		pipe.Del(deleteKeys...)
		revision := fmt.Sprintf("%d", time.Now().Unix())
		pipe.Set(revisionKey, revision, 0)
		_, err = pipe.Exec()

		if err != nil {
			return fmt.Errorf("fail to delete configuration from redis: %s", err)
		}
	}

	return nil
}

func (r *ConfigurationStore) Save(ctx context.Context, req *configuration.SaveRequest) error {
	// step1: get all the keys for this app
	pattern, err := internal.BuildRedisKeyPattern(req.AppID)
	if err != nil {
		return err
	}
	currentKeys, err := r.client.Keys(pattern).Result()
	if err != nil {
		return err
	}

	// step2. prepare redis keys/values to save
	saveRedisKeyValues := make(map[string]map[string]string)
	for _, item := range req.Items {
		if item.Content != "" {
			// save configuration item
			redisKey, err := internal.BuildRedisKey(req.AppID, item.Name)
			if err != nil {
				return fmt.Errorf("fail to build redis key for key=%s: %s", item.Name, err)
			}
			redisValue, err := internal.BuildRedisValue(item.Content, item.Tags)
			if err != nil {
				return fmt.Errorf("fail to build redis value for key=%s: %s", item.Name, err)
			}
			saveRedisKeyValues[redisKey] = redisValue
		}
	}

	// step3: prepare redis keys to delete: key exists in redis but not exist in request
	deleteRedisKeys := make([]string, 0, len(req.Items))
	for _, key := range currentKeys {
		if internal.IsRevisionKey(key) {
			continue
		}
		if _, ok := saveRedisKeyValues[key]; !ok {
			deleteRedisKeys = append(deleteRedisKeys, key)
		}
	}

	// step4. execute all the "key delete"/"key save"/"revision set" in a transaction
	pipe := r.client.TxPipeline()
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

	revisionKey, _ := internal.BuildRedisKey4Revision(req.AppID)
	revision := fmt.Sprintf("%d", time.Now().Unix())
	pipe.Set(revisionKey, revision, 0)
	_, err = pipe.Exec()
	if err != nil {
		return fmt.Errorf("fail to save configuration into redis: %s", err)
	}

	return nil
}
