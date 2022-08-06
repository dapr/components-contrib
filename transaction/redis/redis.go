package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/dapr/components-contrib/transaction"
	"github.com/dapr/kit/logger"

	redis "github.com/go-redis/redis/v8"

	rediscomponent "github.com/dapr/components-contrib/internal/component/redis"
)

const (
	defaultStateStoreDuration = 300
	initializationState       = 0
	commitState               = 1
	rollbackState             = -1
)

type Tcc struct {
	logger         logger.Logger
	client         redis.UniversalClient
	clientSettings *rediscomponent.Settings
	metadata       rediscomponent.Metadata
	cancel         context.CancelFunc
	ctx            context.Context
	duration       int
}

func NewTccTransaction(logger logger.Logger) *Tcc {
	t := &Tcc{
		logger: logger,
	}
	return t
}

// initialize the banch transactions state store
func (t *Tcc) InitTransactionStateStore(metadata transaction.Metadata) error {
	// state store parse config
	m, err := rediscomponent.ParseRedisMetadata(metadata.Properties)
	if err != nil {
		return err
	}
	// verify the  `redisHost`
	if metadata.Properties["redisHost"] == "" {
		return fmt.Errorf("InitTransactionstateStore error: redisHost is empty")
	}
	t.metadata = m

	// initialize the duration
	if m.TTLInSeconds != nil {
		t.duration = *m.TTLInSeconds
	} else {
		t.duration = defaultStateStoreDuration
	}

	// init client
	defaultSettings := rediscomponent.Settings{RedisMaxRetries: m.MaxRetries, RedisMaxRetryInterval: rediscomponent.Duration(m.MaxRetryBackoff)}
	t.client, t.clientSettings, err = rediscomponent.ParseClientFromProperties(metadata.Properties, &defaultSettings)
	if err != nil {
		return err
	}
	t.ctx, t.cancel = context.WithCancel(context.Background())
	// connect to redis
	if _, err = t.client.Ping(t.ctx).Result(); err != nil {
		return fmt.Errorf("InitTransactionstateStore error connecting to redis at %s: %s", t.clientSettings.Host, err)
	}
	return nil
}

func (t *Tcc) DisTransactionStateStore() error {
	// hset
	fmt.Printf("log SubTransactionStateStore")
	nx := t.client.Set(t.ctx, "transaction::test", "test", time.Second*time.Duration(t.duration))
	if nx == nil {
		return fmt.Errorf("transaction store error")
	}
	return nil
}

func (t *Tcc) Init(metadata transaction.Metadata) {
	t.logger.Debug("init tranaction: tcc")
	t.InitTransactionStateStore(metadata)
}

// persistence state of initialization
func (t *Tcc) Begin() {
	t.DisTransactionStateStore()
}

func (t *Tcc) Try() {
	t.logger.Debug("transaction store true")
}

// commit the trasaction and release the state
func (t *Tcc) Commit() {

	t.logger.Info("this is Tcc, I received ")
}

// commit the trasaction and release the state
func (t *Tcc) RollBack() {
	t.logger.Info("this is Tcc, I received ")
}
