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
	defaultBunchTransactionIdPre = "bunch::"
	defaultState                 = 0
	stateForSuccess              = 1
	stateForFailure              = -1
	bunchTransactionTryState     = "state"
	bunchTransacitonTryRequest   = "tryMethodRequest"
	requestStatusOK              = 1
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

// initialize the bunch transactions state store
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

// store all of the distribute transaction id and bunch transaction id into a redis map
func (t *Tcc) InitDisTransactionStateStore(transactionId string, bunchTransactionStateStores map[string]interface{}) error {
	if transactionId == "" || len(bunchTransactionStateStores) == 0 {
		t.logger.Debug("distribute transaction store initialize param error")
		return fmt.Errorf("distribute transaction store initialize param error")
	}
	// persist the transactionID
	IntCmd := t.client.HSet(t.ctx, transactionId, bunchTransactionStateStores)
	if IntCmd == nil {
		return fmt.Errorf("transaction store persistence error")
	}
	t.client.Expire(t.ctx, transactionId, time.Second*time.Duration(t.duration))
	return nil
}

// update a bunch transaction state and requet param
func (t *Tcc) modifyBunchTransactionState(transactionId string, bunchTransactionId string, bunchTransactionStateStore map[string]interface{}) error {

	IntCmd := t.client.HSet(t.ctx, transactionId, bunchTransactionId, bunchTransactionStateStore)
	if IntCmd == nil {
		return fmt.Errorf("transaction store persistence error")
	}
	return nil
}

func (t *Tcc) genDisTransactionId(xid string) string {
	rand.Seed(time.Now().UnixNano())
	return defaultTransactionIdPre + xid + "::" + strconv.Itoa(rand.Intn(100))
}

func (t *Tcc) genBunchTransactionId(index int) string {
	return defaultBunchTransactionIdPre + strconv.Itoa(index)
}

func (t *Tcc) Init(metadata transaction.Metadata) {
	t.logger.Debug("initialize tranaction component")
	t.InitTransactionStateStore(metadata)
}

// Begin a distribute transaction
func (t *Tcc) Begin(beginRequest transaction.BeginTransactionRequest) (*transaction.BeginResponse, error) {
	t.logger.Debug("Begin a distribute transaction")
	if beginRequest.BunchTransactionNum <= 0 {
		return &transaction.BeginResponse{}, fmt.Errorf("must declare a positive number of bunch transactions, but %d given", beginRequest.BunchTransactionNum)
	}
	xid := uuid.Must(uuid.NewV4())
	transactionId := t.genDisTransactionId(xid.String())
	i := 1
	bunchTransactionIds := []string{}
	bunchTransactionStateStores := make(map[string]interface{})
	for i <= beginRequest.BunchTransactionNum {
		// allot a bunch transaction id
		bunchTransactionId := t.genBunchTransactionId(i)

		// set to a default state for nothing have happend and a empty request param
		bunchTransactionStateStore := make(map[string]interface{})
		bunchTransactionStateStore[bunchTransactionTryState] = defaultState
		bunchTransactionStateStore[bunchTransacitonTryRequest] = &transaction.TryTransactionRequest{}
		bunchTransactionStateStores[bunchTransactionId] = bunchTransactionStateStore

		bunchTransactionIds = append(bunchTransactionIds, bunchTransactionId)
		i++
	}
	err := t.InitDisTransactionStateStore(transactionId, bunchTransactionStateStores)
	if err != nil {
		t.logger.Debug("distribute transaction state store error! XID: %s", transactionId)
		return &transaction.BeginResponse{}, err
	}
	return &transaction.BeginResponse{
		TransactionId:       transactionId,
		BunchTransactionIds: bunchTransactionIds,
	}, nil
}

// Try to execute bunch transaction
func (t *Tcc) Try(transactionId string, bunchTransactionId string, statusCode int, tryRequest transaction.TryTransactionRequest) error {
	t.logger.Debug("Try to execute bunch transaction")
	if transactionId == "" || bunchTransactionId == "" {
		t.logger.Info("distribute transaction id or bunch transaction id missing")
		return fmt.Errorf("distribute transaction id or bunch transaction id missing")
	}

	bunchTransactionStateStore := make(map[string]interface{})
	if statusCode == requestStatusOK {
		bunchTransactionStateStore[bunchTransactionTryState] = stateForSuccess
	} else {
		bunchTransactionStateStore[bunchTransactionTryState] = stateForFailure
	}
	bunchTransactionStateStore[bunchTransacitonTryRequest] = &tryRequest

	err := t.modifyBunchTransactionState(transactionId, bunchTransactionId, bunchTransactionStateStore)
	if err != nil {
		return fmt.Errorf("distribute transaction state store error")
	}
	t.logger.Debug("%s - %s bunch transaction state store success", transactionId, bunchTransactionId)
	return nil
}

// Confirm the trasaction and release the state
func (t *Tcc) Confirm() {

	t.logger.Info("this is Tcc, I received ")
}

// RollBack the trasaction and release the state
func (t *Tcc) RollBack() {
	t.logger.Info("this is Tcc, I received ")
}
