package redis

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/dapr/components-contrib/transaction"
	"github.com/dapr/kit/logger"
	redis "github.com/go-redis/redis/v8"
	uuid "github.com/satori/go.uuid"

	rediscomponent "github.com/dapr/components-contrib/internal/component/redis"
)

const (
	defaultStateStoreDuration    = 300
	defaultTransactionIdPre      = "dapr::transaction::"
	defaultBanchTransactionIdPre = "banch::"
	initializationState          = 0
	commitState                  = 1
	rollbackState                = -1
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

//
func (t *Tcc) InitDisTransactionStateStore(transactionId string, branchTransactionIds map[string]interface{}) error {
	if transactionId == "" || len(branchTransactionIds) == 0 {
		t.logger.Debug("distribute transaction store initialize param error")
		return fmt.Errorf("distribute transaction store initialize param error")
	}
	// persist the transactionID
	IntCmd := t.client.HSet(t.ctx, transactionId, branchTransactionIds)
	if IntCmd == nil {
		return fmt.Errorf("transaction store persistence error")
	}
	t.client.Expire(t.ctx, transactionId, time.Second*time.Duration(t.duration))
	return nil
}

func (t *Tcc) genDisTransactionId(xid string) string {
	rand.Seed(time.Now().UnixNano())
	return defaultTransactionIdPre + "::" + xid + "::" + strconv.Itoa(rand.Intn(100))
}

func (t *Tcc) genBanchTransactionId(index int) string {
	return defaultBanchTransactionIdPre + "::" + strconv.Itoa(index)
}

func (t *Tcc) Init(metadata transaction.Metadata) {
	t.logger.Debug("initialize tranaction component")
	t.InitTransactionStateStore(metadata)
}

// Begin a distribute transaction
func (t *Tcc) Begin(beginRequest transaction.BeginTransactionRequest) (*transaction.BeginResponse, error) {
	t.logger.Debug("Begin a distribute transaction")
	if beginRequest.BanchTransactionNum <= 0 {
		return &transaction.BeginResponse{}, fmt.Errorf("Must declare a positive number of banch transactions")
	}
	xid := uuid.Must(uuid.NewV4())
	transactionId := t.genDisTransactionId(xid.String())
	i := 1
	branchTransactionIds := make(map[string]interface{})
	for i <= beginRequest.BanchTransactionNum {
		branchTransactionIds[t.genBanchTransactionId(i)] = initializationState
		i++
	}
	err := t.InitDisTransactionStateStore(transactionId, branchTransactionIds)
	if err != nil {
		t.logger.Debug("distribute transaction state store error! XID: %s", transactionId)
		return &transaction.BeginResponse{}, err
	}
	return &transaction.BeginResponse{
		TransactionId:        transactionId,
		BranchTransactionIds: branchTransactionIds,
	}, nil
}

func (t *Tcc) Try() {
	t.logger.Debug("transaction store true")
}

// Confirm the trasaction and release the state
func (t *Tcc) Confirm() {

	t.logger.Info("this is Tcc, I received ")
}

// RollBack the trasaction and release the state
func (t *Tcc) RollBack() {
	t.logger.Info("this is Tcc, I received ")
}
