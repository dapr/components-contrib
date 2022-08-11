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
	//defaultStateStoreDuration       = 300
	// set a test duration
	defaultStateStoreDuration       = 3000
	defaultTransactionIdPre         = "transaction-"
	defaultBunchTransactionIdPre    = "bunch-"
	defaultState                    = 0
	stateForTrySuccess              = 1
	stateForTryFailure              = -1
	stateForConfirmSuccess          = 2
	stateForConfirmFailure          = -2
	stateForRollBackSuccess         = 3
	stateForRollBackFailure         = -3
	bunchTransactionStateParam      = "state"
	bunchTransacitonTryRequestParam = "tryRequestParam"
	requestStatusOK                 = 1
	defaultTransactionSchema        = "tcc"
)

type DistributeTransaction struct {
	logger         logger.Logger
	client         redis.UniversalClient
	clientSettings *rediscomponent.Settings
	metadata       rediscomponent.Metadata
	cancel         context.CancelFunc
	ctx            context.Context
	duration       int
	schema         string
}

func NewDistributeTransaction(logger logger.Logger) *DistributeTransaction {
	t := &DistributeTransaction{
		logger: logger,
	}
	return t
}

// initialize a redis client which is used for store bunch transactions states
func (t *DistributeTransaction) InitTransactionStateStore(metadata transaction.Metadata) error {
	// parse redis config
	m, err := rediscomponent.ParseRedisMetadata(metadata.Properties)
	if err != nil {
		return err
	}
	if metadata.Properties["redisHost"] == "" {
		return fmt.Errorf("initialize transaction state store error: redisHost is empty")
	}

	t.metadata = m

	// if property TTLInSeconds not specified, use defaultStateStoreDuration as duration
	if m.TTLInSeconds != nil {
		t.duration = *m.TTLInSeconds
	} else {
		t.duration = defaultStateStoreDuration
	}

	// initialize redis client
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

// persist bunch transactions into a redis map
func (t *DistributeTransaction) initBunchTransactionState(transactionId string, bunchTransactionInitStates map[string]interface{}) error {
	t.logger.Debug("start to persit transaction state for ", transactionId)
	if transactionId == "" || len(bunchTransactionInitStates) == 0 {
		t.logger.Debug("transactionId or bunchTransactionInitStates is empty!")
		return fmt.Errorf("distribute transaction store initialize param error")
	}

	IntCmd := t.client.HSet(t.ctx, transactionId, bunchTransactionInitStates)
	if IntCmd.Err() != nil {
		return fmt.Errorf("transaction state store persistence error")
	}
	// update ttl for the map, it doesn't mater get a failed result as it will be deleted when the transaction confirmed or roll back.
	t.client.Expire(t.ctx, transactionId, time.Second*time.Duration(t.duration))
	return nil
}

// modify the bunch transaction state info
func (t *DistributeTransaction) modifyBunchTransactionState(transactionId string, bunchTransactionId string, bunchTransactionStateStore string) error {
	t.logger.Debug("modify transaction state for ", transactionId)
	if transactionId == "" || bunchTransactionId == "" {
		return fmt.Errorf("transaction id or bunch transaction id missing")
	}
	IntCmd := t.client.HSet(t.ctx, transactionId, bunchTransactionId, bunchTransactionStateStore)
	if IntCmd.Err() == nil {
		return fmt.Errorf("transaction store persistence error")
	}
	return nil
}

// read all of the transaction state info,
func (t *DistributeTransaction) getBunchTransactionState(transactionId string) (map[string]int, error) {
	t.logger.Debug("read transaction state for ", transactionId)
	if transactionId == "" {
		return nil, fmt.Errorf("transaction id missing")
	}

	res := t.client.HGetAll(t.ctx, transactionId)
	if res.Err() != nil {
		t.logger.Debug("read transaction state error :", res.Err())
		return nil, fmt.Errorf("read transaction error")
	}

	bunchTransactionStatePersit, err := res.Result()
	if err != nil {
		t.logger.Debug("bunch transaction state info anti-serialization error :", err)
		return nil, fmt.Errorf("bunch transaction state info anti-serialization error")
	}

	bunchTransactionState := make(map[string]int)
	for bunchTransactionId, stateInfo := range bunchTransactionStatePersit {
		parse := t.parseStringToMap(stateInfo)
		stateCode, _ := parse[bunchTransactionStateParam].(int)
		bunchTransactionState[bunchTransactionId] = stateCode
	}
	return bunchTransactionState, nil

}

func (t *DistributeTransaction) getBunchTransactions(transactionId string) (map[string]map[string]interface{}, error) {
	t.logger.Debug("read transaction state for ", transactionId)
	if transactionId == "" {
		return nil, fmt.Errorf("transaction id missing")
	}

	res := t.client.HGetAll(t.ctx, transactionId)
	if res.Err() != nil {
		t.logger.Debug("read transaction state error :", res.Err())
		return nil, fmt.Errorf("read transaction error")
	}

	bunchTransactionStatePersit, err := res.Result()
	if err != nil {
		t.logger.Debug("bunch transaction state info anti-serialization error :", err)
		return nil, fmt.Errorf("bunch transaction state info anti-serialization error")
	}

	bunchTransactions := make(map[string]map[string]interface{})
	for bunchTransactionId, stateInfo := range bunchTransactionStatePersit {
		bunchTransactions[bunchTransactionId] = t.parseStringToMap(stateInfo)
	}
	return bunchTransactions, nil

}

func (t *DistributeTransaction) releaseBunchTransactionState(transactionId string) error {
	if transactionId == "" {
		return fmt.Errorf("transaction id missing")
	}
	res := t.client.HDel(t.ctx, transactionId)
	if res.Err() != nil {
		t.logger.Debug("release transaction from persistent store error", res.Err())
		return fmt.Errorf("release transaction from persistent store error")
	}
	return nil
}

func (t *DistributeTransaction) parseMapToString(param map[string]interface{}) string {
	parse, err := json.Marshal(param)
	if err != nil {
		t.logger.Info("param parse to string something error")
		return ""
	}
	return string(parse)
}

func (t *DistributeTransaction) parseStringToMap(param string) map[string]interface{} {
	var parse map[string]interface{}
	err := json.Unmarshal([]byte(param), &parse)
	if err != nil {
		return nil
	}
	return parse
}

// generate a distribute transaction id with uuid and random number
func (t *DistributeTransaction) genDisTransactionId() string {
	xid := uuid.Must(uuid.NewV4()).String()
	rand.Seed(time.Now().UnixNano())
	return defaultTransactionIdPre + xid + "-" + strconv.Itoa(rand.Intn(10000))
}

// generate a bunch transaction id
func (t *DistributeTransaction) genBunchTransactionId(index int) string {
	return defaultBunchTransactionIdPre + strconv.Itoa(index)
}

func (t *DistributeTransaction) Init(metadata transaction.Metadata) {
	t.logger.Debug("initialize tranaction component")
	if metadata.Properties["schema"] != "" {
		t.schema = metadata.Properties["schema"]
	} else {
		t.schema = defaultTransactionSchema
	}
	t.InitTransactionStateStore(metadata)
}

// when begin a distribute transaction, generate a transactionId and the number of bunchTransactionId which is defined by BunchTransactionNum, persist store these into a redis map
func (t *DistributeTransaction) Begin(beginRequest transaction.BeginTransactionRequest) (*transaction.BeginResponse, error) {
	t.logger.Debug("Begin a distribute transaction")
	if beginRequest.BunchTransactionNum <= 0 {
		return nil, fmt.Errorf("must declare a positive number of bunch transactions, but %d given", beginRequest.BunchTransactionNum)
	}
	transactionId := t.genDisTransactionId()

	bunchTransactionIds := []string{}
	bunchTransactionStateStores := make(map[string]interface{})

	i := 1
	for i <= beginRequest.BunchTransactionNum {
		// allot a bunch transaction id
		bunchTransactionId := t.genBunchTransactionId(i)

		// set to a default state for nothing have happend and a empty request param
		bunchTransactionStateStore := make(map[string]interface{})
		bunchTransactionStateStore[bunchTransactionStateParam] = defaultState
		bunchTransactionStateStore[bunchTransacitonTryRequestParam] = &transaction.TransactionTryRequestParam{}
		bunchTransactionStateStores[bunchTransactionId] = t.parseMapToString(bunchTransactionStateStore)

		bunchTransactionIds = append(bunchTransactionIds, bunchTransactionId)
		i++
	}

	t.logger.Debug("transaction id is :", transactionId)

	err := t.initBunchTransactionState(transactionId, bunchTransactionStateStores)
	if err != nil {
		t.logger.Debug("distribute transaction state store error! XID: %s", transactionId)
		return nil, err
	}
	t.logger.Debug("a new distribute transaction start")
	return &transaction.BeginResponse{
		TransactionId:       transactionId,
		BunchTransactionIds: bunchTransactionIds,
	}, nil
}

// Try to execute bunch transaction
func (t *DistributeTransaction) Try(tryRequest transaction.BunchTransactionTryRequest) error {
	t.logger.Debug("Try to execute bunch transaction")
	if tryRequest.TransactionId == "" || tryRequest.BunchTransactionId == "" {
		t.logger.Info("distribute transaction id or bunch transaction id missing")
		return fmt.Errorf("distribute transaction id or bunch transaction id missing")
	}

	bunchTransactionStateStore := make(map[string]interface{})
	if tryRequest.StatusCode == requestStatusOK {
		bunchTransactionStateStore[bunchTransactionStateParam] = stateForTrySuccess
	} else {
		bunchTransactionStateStore[bunchTransactionStateParam] = stateForTryFailure
	}
	bunchTransactionStateStore[bunchTransacitonTryRequestParam] = &tryRequest.TryRequestParam

	err := t.modifyBunchTransactionState(tryRequest.TransactionId, tryRequest.BunchTransactionId, t.parseMapToString(bunchTransactionStateStore))
	if err != nil {
		return fmt.Errorf("distribute transaction state store error")
	}
	t.logger.Debug(tryRequest.TransactionId, "bunch transaction state store success")
	return nil
}

// Confirm a bunch trasaction
func (t *DistributeTransaction) Confirm(confirmRequest transaction.BunchTransactionConfirmRequest) error {
	t.logger.Debug("Confirm the bunch transaction")
	if confirmRequest.TransactionId == "" || confirmRequest.BunchTransactionId == "" {
		t.logger.Debug("distribute transaction id or bunch transaction id missing")
		return fmt.Errorf("distribute transaction id or bunch transaction id missing")
	}
	bunchTransactionStateStore := make(map[string]interface{})
	if confirmRequest.StatusCode == requestStatusOK {
		bunchTransactionStateStore[bunchTransactionStateParam] = stateForConfirmSuccess
	} else {
		bunchTransactionStateStore[bunchTransactionStateParam] = stateForConfirmFailure
	}

	err := t.modifyBunchTransactionState(confirmRequest.TransactionId, confirmRequest.BunchTransactionId, t.parseMapToString(bunchTransactionStateStore))
	if err != nil {
		return fmt.Errorf("distribute transaction state store error")
	}
	t.logger.Debug(confirmRequest.TransactionId, "bunch transaction state store success")
	return nil
}

// RollBack the trasaction
func (t *DistributeTransaction) RollBack(rollBackRequest transaction.BunchTransactionRollBackRequest) error {
	t.logger.Debug("RollBack the bunch transaction")
	if rollBackRequest.TransactionId == "" || rollBackRequest.BunchTransactionId == "" {
		t.logger.Info("distribute transaction id or bunch transaction id missing")
		return fmt.Errorf("distribute transaction id or bunch transaction id missing")
	}
	bunchTransactionStateStore := make(map[string]interface{})
	if rollBackRequest.StatusCode == requestStatusOK {
		bunchTransactionStateStore[bunchTransactionStateParam] = stateForRollBackSuccess
	} else {
		bunchTransactionStateStore[bunchTransactionStateParam] = stateForRollBackFailure
	}

	err := t.modifyBunchTransactionState(rollBackRequest.TransactionId, rollBackRequest.BunchTransactionId, t.parseMapToString(bunchTransactionStateStore))
	if err != nil {
		return fmt.Errorf("distribute transaction state store error")
	}
	t.logger.Debug(rollBackRequest.TransactionId, "bunch transaction state store success")
	return nil
}

// get all bunch transaction state of the distribute transaction
func (t *DistributeTransaction) GetBunchTransactionState(transactionReq transaction.GetBunchTransactionsRequest) (*transaction.TransactionStateResponse, error) {
	if transactionReq.TransactionId == "" {
		t.logger.Debug("distribute transaction id missing")
		return nil, fmt.Errorf("distribute transaction id missing")
	}

	bunchTransactionState, err := t.getBunchTransactionState(transactionReq.TransactionId)
	if err != nil {
		return nil, err
	}
	return &transaction.TransactionStateResponse{
		TransactionId:          transactionReq.TransactionId,
		BunchTransactionStates: bunchTransactionState,
	}, nil
}

func (t *DistributeTransaction) GetBunchTransactions(transactionReq transaction.GetBunchTransactionsRequest) (*transaction.BunchTransactionsResponse, error) {
	if transactionReq.TransactionId == "" {
		t.logger.Debug("distribute transaction id missing")
		return nil, fmt.Errorf("distribute transaction id missing")
	}

	bunchTransactions, err := t.getBunchTransactions(transactionReq.TransactionId)
	if err != nil {
		return nil, err
	}
	return &transaction.BunchTransactionsResponse{
		TransactionId:     transactionReq.TransactionId,
		BunchTransactions: bunchTransactions,
	}, nil
}

// release transaction state store when all of the bunch transaction confirmed or roll back
func (t *DistributeTransaction) ReleaseTransactionResource(releaseRequest transaction.ReleaseTransactionRequest) error {
	if releaseRequest.TransactionId == "" {
		t.logger.Info("distribute transaction id missing")
		return fmt.Errorf("distribute transaction id missing")
	}
	err := t.releaseBunchTransactionState(releaseRequest.TransactionId)
	if err != nil {
		return err
	}
	return nil
}
