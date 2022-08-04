package transaction

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"

	rediscomponent "github.com/dapr/components-contrib/internal/component/redis"
)

type Transaction interface {
	// Init this component.
	Init(metadata Metadata)

	Try()
	// commit a distribute transaction
	Commit()

	// rooback a distribute transaction
	RollBack()
}

func InitTransactionStateStore(metadata Metadata) (error, *redis.UniversalClient, context.Context, context.CancelFunc) {
	// 1. parse config
	m, err := rediscomponent.ParseRedisMetadata(metadata.Properties)
	if err != nil {
		return err, nil
	}
	// verify the  `redisHost`
	if metadata.Properties["redisHost"] == "" {
		return fmt.Errorf("InitTransactionstateStore error: redisHost is empty"), nil, nil, nil
	}

	// 2. init client
	defaultSettings := rediscomponent.Settings{RedisMaxRetries: m.MaxRetries, RedisMaxRetryInterval: rediscomponent.Duration(m.MaxRetryBackoff)}
	client, clientSettings, err := rediscomponent.ParseClientFromProperties(metadata.Properties, &defaultSettings)
	if err != nil {
		return err, nil, nil, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	// 3. connect to redis
	if _, err = client.Ping(ctx).Result(); err != nil {
		return fmt.Errorf("InitTransactionstateStore error connecting to redis at %s: %s", clientSettings.Host, err), nil, nil, nil
	}
	return nil, &client, ctx, cancel
}
