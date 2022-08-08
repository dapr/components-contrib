package redis

import (
	"context"
	"encoding/json"
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
	defaultStateStoreDuration       = 300
	defaultTransactionIdPre         = "dapr::transaction::"
	defaultBunchTransactionIdPre    = "bunch::"
	defaultState                    = 0
	stateForSuccess                 = 1
	stateForFailure                 = -1
	bunchTransactionTryState        = "state"
	bunchTransacitonTryRequestParam = "tryRequestParam"
	requestStatusOK                 = 1
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
	t.logger.Debug("start to persit init transaction state")
	IntCmd := t.client.HSet(t.ctx, transactionId, bunchTransactionStateStores)
	if IntCmd.Err() != nil {
		return fmt.Errorf("transaction store persistence error")
	}
	t.client.Expire(t.ctx, transactionId, time.Second*time.Duration(t.duration))
	return nil
}

// update a bunch transaction state and requet param
func (t *Tcc) modifyBunchTransactionState(transactionId string, bunchTransactionId string, bunchTransactionStateStore string) error {
	if transactionId == "" || bunchTransactionId == "" {
		return fmt.Errorf("transaction id or bunch transaction id missing")
	}
	IntCmd := t.client.HSet(t.ctx, transactionId, bunchTransactionId, bunchTransactionStateStore)
	if IntCmd.Err() == nil {
		return fmt.Errorf("transaction store persistence error")
	}
	return nil
}

func (t *Tcc) getBunchTransactionState(transactionId string) (map[string]int, error) {
	if transactionId == "" {
		return make(map[string]int), fmt.Errorf("transaction id missing")
	}
	t.logger.Debug("try to get bunch transactions")

	res := t.client.HGetAll(t.ctx, transactionId)
	if res.Err() != nil {
		t.logger.Debug("read transaction from persistent store error")
		return make(map[string]int), fmt.Errorf("read transaction from persistent store error")
	}

	var bunchTransactionStatePersit map[string]string
	if err := res.Scan(&bunchTransactionStatePersit); err != nil {
		t.logger.Debug("bunch transaction state info anti-serialization error")
		return make(map[string]int), fmt.Errorf("bunch transaction state info anti-serialization error")
	}

	t.logger.Debug(bunchTransactionStatePersit)

	bunchTransactionState := make(map[string]int)
	for bunchTransactionId, stateInfo := range bunchTransactionStatePersit {
		t.logger.Debug(bunchTransactionId)
		t.logger.Debug(stateInfo)
		bunchTransactionState[bunchTransactionId] = 1
	}
	return bunchTransactionState, nil

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

func (t *Tcc) parseMapToString(param map[string]interface{}) string {
	parse, err := json.Marshal(param)
	if err != nil {
		t.logger.Info("param parse to string something error")
		return ""
	}
	return string(parse)
}

func (t *Tcc) parseStringToMap(param string) map[string]interface{} {
	var parse map[string]interface{}
	err := json.Unmarshal([]byte(param), &parse)
	if err != nil {
		return make(map[string]interface{})
	}
	return parse
}

// Begin a distribute transaction
func (t *Tcc) Begin(beginRequest transaction.BeginTransactionRequest) (*transaction.BeginResponse, error) {
	t.logger.Debug("Begin a distribute transaction")
	if beginRequest.BunchTransactionNum <= 0 {
		return &transaction.BeginResponse{}, fmt.Errorf("must declare a positive number of bunch transactions, but %d given", beginRequest.BunchTransactionNum)
	}
	xid := uuid.Must(uuid.NewV4())
	transactionId := t.genDisTransactionId(xid.String())

	bunchTransactionIds := []string{}
	bunchTransactionStateStores := make(map[string]interface{})

	i := 1
	for i <= beginRequest.BunchTransactionNum {
		// allot a bunch transaction id
		bunchTransactionId := t.genBunchTransactionId(i)

		// set to a default state for nothing have happend and a empty request param
		bunchTransactionStateStore := make(map[string]interface{})
		bunchTransactionStateStore[bunchTransactionTryState] = defaultState
		bunchTransactionStateStore[bunchTransacitonTryRequestParam] = &transaction.TransactionTryRequestParam{}
		bunchTransactionStateStores[bunchTransactionId] = t.parseMapToString(bunchTransactionStateStore)

		bunchTransactionIds = append(bunchTransactionIds, bunchTransactionId)
		i++
	}

	t.logger.Debug("transaction id is :", transactionId)
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
func (t *Tcc) Try(tryRequest transaction.BunchTransactionTryRequest) error {
	t.logger.Debug("Try to execute bunch transaction")
	if tryRequest.TransactionId == "" || tryRequest.BunchTransactionId == "" {
		t.logger.Info("distribute transaction id or bunch transaction id missing")
		return fmt.Errorf("distribute transaction id or bunch transaction id missing")
	}

	bunchTransactionStateStore := make(map[string]interface{})
	if tryRequest.StatusCode == requestStatusOK {
		bunchTransactionStateStore[bunchTransactionTryState] = stateForSuccess
	} else {
		bunchTransactionStateStore[bunchTransactionTryState] = stateForFailure
	}
	bunchTransactionStateStore[bunchTransacitonTryRequestParam] = &tryRequest.TryRequestParam

	err := t.modifyBunchTransactionState(tryRequest.TransactionId, tryRequest.BunchTransactionId, t.parseMapToString(bunchTransactionStateStore))
	if err != nil {
		return fmt.Errorf("distribute transaction state store error")
	}
	t.logger.Debug("%s - %s bunch transaction state store success", tryRequest.TransactionId, tryRequest.BunchTransactionId)
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

// get all bunch transaction state of the distribute transaction
func (t *Tcc) GetBunchTransactions(transactionReq transaction.GetBunchTransactionsRequest) (*transaction.TransactionStateResponse, error) {
	if transactionReq.TransactionId == "" {
		t.logger.Info("distribute transaction id missing")
		return &transaction.TransactionStateResponse{}, fmt.Errorf("distribute transaction id missing")
	}
	xid := transactionReq.TransactionId
	t.logger.Debug("input :", transactionReq)
	t.logger.Debug("distribute transaction id is ", xid)
	bunchTransactionState, err := t.getBunchTransactionState(xid)
	if err != nil {
		return &transaction.TransactionStateResponse{}, err
	}
	return &transaction.TransactionStateResponse{
		TransactionId:          xid,
		BunchTransactionStates: bunchTransactionState,
	}, nil
}
