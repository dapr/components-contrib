package redis

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-redis/redis/v8"

	"github.com/dapr/components-contrib/configuration"
	"github.com/dapr/components-contrib/tests/utils/configupdater"
	"github.com/dapr/kit/logger"
)

const (
	host      = "redisHost"
	password  = "redisPassword"
	defaultDB = 0
	separator = "||"
)

type metadata struct {
	Host     string
	Password string
}

type ConfigUpdater struct {
	client redis.UniversalClient

	logger logger.Logger
}

func NewRedisConfigUpdater(logger logger.Logger) configupdater.Updater {
	return &ConfigUpdater{
		logger: logger,
	}
}

func parseRedisMetadata(props map[string]string) (metadata, error) {
	m := metadata{}

	if val, ok := props[host]; ok && val != "" {
		m.Host = val
	} else {
		return m, errors.New("redis store error: missing host address")
	}

	if val, ok := props[password]; ok {
		m.Password = val
	}
	return m, nil
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
	m, err := parseRedisMetadata(props)
	if err != nil {
		return err
	}

	opts := &redis.Options{
		Addr:     m.Host,
		Password: m.Password,
		DB:       defaultDB,
	}

	r.client = redis.NewClient(opts)

	if _, err = r.client.Ping(context.TODO()).Result(); err != nil {
		return fmt.Errorf("redis store: error connecting to redis at %s: %s", m.Host, err)
	}

	return nil
}

func (r *ConfigUpdater) AddKey(items map[string]*configuration.Item) error {
	values := getRedisValuesFromItems(items)

	err := r.client.MSet(context.Background(), values).Err()

	return err
}

func (r *ConfigUpdater) UpdateKey(items map[string]*configuration.Item) error {
	return r.AddKey(items)
}

func (r *ConfigUpdater) DeleteKey(keys []string) error {
	err := r.client.Del(context.Background(), keys...).Err()

	return err
}
