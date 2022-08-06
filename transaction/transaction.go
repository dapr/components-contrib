package transaction

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"

	rediscomponent "github.com/dapr/components-contrib/internal/component/redis"
)

type Transaction interface {

	// Init this component.
	Init(metadata Metadata)

	// Begin a distribute transaction
	Begin()

	// try to lock the transaction resource
	Try()

	// commit a distribute transaction
	Commit()

	// rooback a distribute transaction
	RollBack()
}

type TransactionStateStore struct {
	client         redis.UniversalClient
	clientSettings *rediscomponent.Settings
	metadata       rediscomponent.Metadata
	cancel         context.CancelFunc
	ctx            context.Context
	duration       int
}

const (
	defaultStateStoreDuration = 300
	initializationState       = 0
	commitState               = 1
	rollbackState             = -1
)

// initialize the banch transactions state store
func (ts *TransactionStateStore) InitTransactionStateStore(metadata Metadata) error {
	// state store parse config
	m, err := rediscomponent.ParseRedisMetadata(metadata.Properties)
	if err != nil {
		return err
	}
	// verify the  `redisHost`
	if metadata.Properties["redisHost"] == "" {
		return fmt.Errorf("InitTransactionstateStore error: redisHost is empty")
	}
	ts.metadata = m

	// initialize the duration
	if m.TTLInSeconds != nil {
		ts.duration = *m.TTLInSeconds
	} else {
		ts.duration = defaultStateStoreDuration
	}

	// init client
	defaultSettings := rediscomponent.Settings{RedisMaxRetries: m.MaxRetries, RedisMaxRetryInterval: rediscomponent.Duration(m.MaxRetryBackoff)}
	ts.client, ts.clientSettings, err = rediscomponent.ParseClientFromProperties(metadata.Properties, &defaultSettings)
	if err != nil {
		return err
	}
	ts.ctx, ts.cancel = context.WithCancel(context.Background())
	// connect to redis
	if _, err = ts.client.Ping(ts.ctx).Result(); err != nil {
		return fmt.Errorf("InitTransactionstateStore error connecting to redis at %s: %s", ts.clientSettings.Host, err)
	}
	return nil
}

func (ts *TransactionStateStore) DisTransactionStateStore() error {
	// hset
	fmt.Printf("log SubTransactionStateStore")
	nx := ts.client.Set(ts.ctx, "transaction::test", "test", time.Second*time.Duration(ts.duration))
	if nx == nil {
		return fmt.Errorf("transaction store error")
	}
	return nil
}
