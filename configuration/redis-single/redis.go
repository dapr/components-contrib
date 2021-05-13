// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis_single

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dapr/components-contrib/configuration"
	"github.com/dapr/components-contrib/configuration/redis-single/internal"
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
	redisKey, err := internal.BuildRedisKey(req.AppID)
	if err != nil {
		return nil, err
	}

	bytes, err := r.client.Get(redisKey).Bytes()
	if err != nil {
		return nil, err
	}

	c := &configuration.Configuration{}
	err = json.Unmarshal(bytes, c)
	if err != nil {
		return nil, err
	}

	return &configuration.GetResponse {
		AppID: req.AppID,
		Revision: c.Revision,
		Items: c.Items,
	}, nil
}

func (r *ConfigurationStore) Subscribe(ctx context.Context, req *configuration.SubscribeRequest, handler configuration.UpdateHandler) error {
	redisKey, err := internal.BuildRedisKey(req.AppID)
	if err != nil {
		return err
	}

	go r.startSubscribe(ctx, req, handler, redisKey)
	return nil
}

func (r *ConfigurationStore) startSubscribe(ctx context.Context, req *configuration.SubscribeRequest, handler configuration.UpdateHandler, redisKey string) error {
	// enable notify-keyspace-events by redis Set command
	r.client.ConfigSet("notify-keyspace-events", "KA")
	channel := fmt.Sprintf("__keyspace*__:%s", redisKey)
	p := r.client.PSubscribe(channel)

	for msg := range p.Channel() {
		redisKey, err := internal.ParseRedisKeyFromEvent(msg.Channel)
		if err != nil {
			continue
		}

		appID, err := internal.ParseRedisKey(redisKey)
		if err != nil {
			continue
		}
		if appID != req.AppID {
			continue
		}

		getRequest := configuration.GetRequest{
			AppID: req.AppID,
			Metadata: req.Metadata,
		}
		getResponse, err := r.Get(ctx, &getRequest)
		if err != nil {
			continue
		}

		//TODO: filter by keys in req
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

	return nil
}

func (r *ConfigurationStore) Delete(ctx context.Context, req *configuration.DeleteRequest) error {
	saveRequest := &configuration.SaveRequest{
		AppID:    req.AppID,
		Items: []*configuration.Item{},
	}
	return r.Save(ctx, saveRequest)
}

func (r *ConfigurationStore) Save(ctx context.Context, req *configuration.SaveRequest) error {
	c := configuration.Configuration{
		AppID:    req.AppID,
		Revision: fmt.Sprintf("%d", time.Now().Unix()),
		Items:    req.Items,
	}

	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	redisKey, err := internal.BuildRedisKey(req.AppID)
	if err != nil {
		return err
	}

	_, err = r.client.Set(redisKey, b, 0).Result()
	if err != nil {
		return err
	}

	return nil
}
