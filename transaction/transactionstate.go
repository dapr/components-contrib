package transaction

import (
	"context"
	"fmt"

	"github.com/dapr/components-contrib/transaction"
	"github.com/go-redis/redis/v8"

	rediscomponent "github.com/dapr/components-contrib/internal/component/redis"
)

func InitTransactionstateStore(client *redis.UniversalClient, metadata transaction.Metadata) error {
	// 1. parse config
	m, err := rediscomponent.ParseRedisMetadata(metadata.Properties)
	if err != nil {
		return err
	}
	// verify the  `redisHost`
	if metadata.Properties["redisHost"] == "" {
		return fmt.Errorf("InitTransactionstateStore error: redisHost is empty")
	}

	// 2. init client
	var clientSettings *rediscomponent.Settings
	defaultSettings := rediscomponent.Settings{RedisMaxRetries: m.MaxRetries, RedisMaxRetryInterval: rediscomponent.Duration(m.MaxRetryBackoff)}
	client, clientSettings, err = rediscomponent.ParseClientFromProperties(metadata.Properties, &defaultSettings)
	if err != nil {
		return err
	}
	ctx, _ := context.WithCancel(context.Background())
	// 3. connect to redis
	if _, err = client.Ping(ctx).Result(); err != nil {
		return fmt.Errorf("[standaloneRedisLock]: error connecting to redis at %s: %s", clientSettings.Host, err)
	}
	return nil
}
