package redis

import (
	"context"
	"fmt"

	"github.com/dapr/components-contrib/configuration"
	rediscomponent "github.com/dapr/components-contrib/internal/component/redis"
	"github.com/dapr/components-contrib/tests/utils/configupdater"
	"github.com/dapr/kit/logger"
)

const (
	host      = "redisHost"
	password  = "redisPassword"
	separator = "||"
)

type ConfigUpdater struct {
	client         rediscomponent.RedisClient
	clientSettings *rediscomponent.Settings

	logger logger.Logger
}

func NewRedisConfigUpdater(logger logger.Logger) configupdater.Updater {
	return &ConfigUpdater{
		logger: logger,
	}
}

func getRedisValueFromItem(item *configuration.Item) string {
	val := item.Value + separator + item.Version
	return val
}

func getRedisValuesFromItems(items map[string]*configuration.Item) map[string]string {
	m := make(map[string]string)

	for key, item := range items {
		val := getRedisValueFromItem(item)
		m[key] = val
	}

	return m
}

func (r *ConfigUpdater) Init(props map[string]string) error {
	var err error
	r.client, r.clientSettings, err = rediscomponent.ParseClientFromProperties(props, nil)
	if err != nil {
		return err
	}

	if _, err = r.client.PingResult(context.TODO()); err != nil {
		return fmt.Errorf("redis store: error connecting to redis at %s: %s", r.clientSettings.Host, err)
	}

	return nil
}

func (r *ConfigUpdater) AddKey(items map[string]*configuration.Item) error {
	values := getRedisValuesFromItems(items)

	for key, val := range values {
		err := r.client.DoWrite(context.Background(), "SET", key, val)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *ConfigUpdater) UpdateKey(items map[string]*configuration.Item) error {
	return r.AddKey(items)
}

func (r *ConfigUpdater) DeleteKey(keys []string) error {
	err := r.client.Del(context.Background(), keys...)

	return err
}
