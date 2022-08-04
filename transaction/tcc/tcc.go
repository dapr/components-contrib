package tcc

import (
	"time"

	"github.com/dapr/components-contrib/transaction"
	"github.com/dapr/kit/logger"

	"github.com/go-redis/redis/v8"

	rediscomponent "github.com/dapr/components-contrib/internal/component/redis"
)

type Tcc struct {
	logger logger.Logger

	client         redis.UniversalClient
	clientSettings *rediscomponent.Settings
	metadata       rediscomponent.Metadata
}

func NewTccTransaction(logger logger.Logger) *Tcc {
	t := &Tcc{
		logger: logger,
	}

	return t
}

func (t *Tcc) Init(metadata transaction.Metadata) {
	_, t.client = transaction.InitTransactionStateStore(metadata)
}

func (t *Tcc) Try() {
	nx := t.client.Set("transaction::test", "test", time.Second*time.Duration(300))
	if nx == nil {
		t.logger.Debug("transaction store error")
	}
	t.logger.Debug("transaction store true")
}

func (t *Tcc) Commit() {
	t.logger.Info("this is Tcc, I received ")
}

func (t *Tcc) RollBack() {
	t.logger.Info("this is Tcc, I received ")
}
