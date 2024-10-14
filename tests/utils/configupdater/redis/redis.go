package redis

import (
	"context"
	"fmt"

	rediscomponent "github.com/dapr/components-contrib/common/component/redis"
	"github.com/dapr/components-contrib/configuration"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/tests/utils/configupdater"
	"github.com/dapr/kit/logger"
)

const (
	host      = "redisHost"
	password  = "redisPassword"
	separator = "||"
)

type ConfigUpdater struct {
	Client rediscomponent.RedisClient

	clientSettings *rediscomponent.Settings
	logger         logger.Logger
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

func getRedisValuesFromItems(items map[string]*configuration.Item) []interface{} {
	m := make([]interface{}, 0, 2*len(items)+1)

	for key, item := range items {
		val := getRedisValueFromItem(item)
		m = append(m, key, val)
	}

	return m
}

func (r *ConfigUpdater) Init(props map[string]string) error {
	var err error
	ctx := context.Background()
	r.Client, r.clientSettings, err = rediscomponent.ParseClientFromProperties(props, metadata.ConfigurationStoreType, ctx, &r.logger)
	if err != nil {
		return err
	}

	if _, err = r.Client.PingResult(context.TODO()); err != nil {
		return fmt.Errorf("redis store: error connecting to redis at %s: %s", r.clientSettings.Host, err)
	}

	return nil
}

func (r *ConfigUpdater) AddKey(items map[string]*configuration.Item) error {
	values := getRedisValuesFromItems(items)
	values = append([]interface{}{"MSET"}, values...)

	err := r.Client.DoWrite(context.Background(), values...)

	return err
}

func (r *ConfigUpdater) UpdateKey(items map[string]*configuration.Item) error {
	return r.AddKey(items)
}

func (r *ConfigUpdater) DeleteKey(keys []string) error {
	err := r.Client.Del(context.Background(), keys...)

	return err
}
