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
	defaultStateStoreDuration    = 300
	defaultTransactionIDPre      = "transaction-"
	defaultBunchTransactionIDPre = "bunch-"
	defaultState                 = 0
	stateForTrySuccess           = 10
	stateForTryFailure           = 1
	stateForConfirmSuccess       = 20
	stateForConfirmFailure       = 2
	stateForRollbackSuccess      = 30
	stateForRollbackFailure      = 3
	requestStatusOK              = 1
	defaultTransactionSchema     = "tcc"
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
	retryTimes     int
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
func (t *DistributeTransaction) initBunchTransactionState(transactionID string, bunchTransactionInitStates []string) error {
	t.logger.Debug("start to persit transaction state for ", transactionID)
	if transactionID == "" || len(bunchTransactionInitStates) == 0 {
		t.logger.Debug("transactionID or bunchTransactionInitStates is empty!")
		return fmt.Errorf("distribute transaction store initialize param error")
	}
	t.logger.Debugf("bunch transaction state :%s", bunchTransactionInitStates)

	IntCmd := t.client.HSet(t.ctx, transactionID, bunchTransactionInitStates)
	if IntCmd.Err() != nil {
		return fmt.Errorf("transaction state store persistence error")
	}
	// update ttl for the map, it doesn't mater get a failed result as it will be deleted when the transaction confirmed or roll back.
	t.client.Expire(t.ctx, transactionID, time.Second*time.Duration(t.duration))
	return nil
}

// modify the bunch transaction state info
func (t *DistributeTransaction) modifyBunchTransactionState(transactionID string, bunchTransactionID string, bunchTransactionStateStore string) error {
	t.logger.Debug("modify transaction state for ", transactionID)
	if transactionID == "" || bunchTransactionID == "" {
		return fmt.Errorf("transaction id or bunch transaction id missing")
	}

	t.logger.Debugf("update transactionID: %s, bunchTransactionID %s set to %s ", transactionID, bunchTransactionID, bunchTransactionStateStore)
	IntCmd := t.client.HSet(t.ctx, transactionID, bunchTransactionID, bunchTransactionStateStore)
	if IntCmd.Err() == nil {
		return fmt.Errorf("transaction store persistence error")
	}
	return nil
}

// read all of the transaction state info,
func (t *DistributeTransaction) getBunchTransactionState(transactionID string) (map[string]int, error) {
	t.logger.Debug("read transaction state for ", transactionID)
	if transactionID == "" {
		return nil, fmt.Errorf("transaction id missing")
	}

	res := t.client.HGetAll(t.ctx, transactionID)
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
	for bunchTransactionID, stateInfo := range bunchTransactionStatePersit {
		t.logger.Debugf("state of bunch transaction: %s is %s ", bunchTransactionID, stateInfo)
		bunchTransaction := transaction.DistributeTransactionState{}
		err = t.parseStringToStruct(stateInfo, &bunchTransaction)
		if err != nil {
			return nil, err
		}
		t.logger.Debug("state parse res: ", bunchTransaction)
		t.logger.Debugf("state result is: %d", bunchTransaction.StatusCode)

		bunchTransactionState[bunchTransactionID] = bunchTransaction.StatusCode
	}
	return bunchTransactionState, nil
}

func (t *DistributeTransaction) getBunchTransactions(transactionID string) (map[string]transaction.DistributeTransactionState, error) {
	t.logger.Debug("read transaction state for ", transactionID)
	if transactionID == "" {
		return nil, fmt.Errorf("transaction id missing")
	}

	res := t.client.HGetAll(t.ctx, transactionID)
	if res.Err() != nil {
		t.logger.Debug("read transaction state error :", res.Err())
		return nil, fmt.Errorf("read transaction error")
	}

	bunchTransactionStatePersit, err := res.Result()
	if err != nil {
		t.logger.Debug("bunch transaction state info anti-serialization error :", err)
		return nil, fmt.Errorf("bunch transaction state info anti-serialization error")
	}

	bunchTransactions := make(map[string]transaction.DistributeTransactionState)
	for bunchTransactionID, stateInfo := range bunchTransactionStatePersit {
		transaction := transaction.DistributeTransactionState{}
		err = t.parseStringToStruct(stateInfo, &transaction)
		if err != nil {
			continue
		}
		bunchTransactions[bunchTransactionID] = transaction
	}
	return bunchTransactions, nil
}

func (t *DistributeTransaction) getBunchTransaction(transactionID string, bunchTransactionID string) (*transaction.DistributeTransactionState, error) {
	t.logger.Debugf("get transaction info for %s - %s", transactionID, bunchTransactionID)
	if transactionID == "" || bunchTransactionID == "" {
		return &transaction.DistributeTransactionState{}, fmt.Errorf("transaction id or bunch transaction id missing")
	}

	res := t.client.HGet(t.ctx, transactionID, bunchTransactionID)
	if res.Err() != nil {
		return &transaction.DistributeTransactionState{}, fmt.Errorf("transaction store persistence error")
	}
	bunchTransactionPersit, err := res.Result()
	if err != nil {
		t.logger.Debug("bunch transaction info anti-serialization error :", err)
		return &transaction.DistributeTransactionState{}, fmt.Errorf("bunch transaction info anti-serialization error")
	}
	bunchTransaction := transaction.DistributeTransactionState{}
	err = t.parseStringToStruct(bunchTransactionPersit, &bunchTransaction)
	if err != nil {
		t.logger.Debugf("bunchTransactionPersit parse error:", err)
		return &transaction.DistributeTransactionState{}, err
	}
	t.logger.Debugf("state of %s is :", bunchTransactionID, bunchTransaction)
	return &bunchTransaction, nil
}

func (t *DistributeTransaction) releaseBunchTransactionState(transactionID string) error {
	if transactionID == "" {
		return fmt.Errorf("transaction id missing")
	}
	res := t.client.HDel(t.ctx, transactionID)
	if res.Err() != nil {
		t.logger.Debug("release transaction from persistent store error", res.Err())
		return fmt.Errorf("release transaction from persistent store error")
	}
	return nil
}

func (t *DistributeTransaction) parseStructToString(param any) string {
	b, err := json.Marshal(param)
	if err != nil {
		return ""
	}
	return string(b)
}

func (t *DistributeTransaction) parseStringToStruct(param string, dest any) error {
	err := json.Unmarshal([]byte(param), dest)
	if err != nil {
		return err
	}
	return nil
}

// generate a distribute transaction id with uuid and random number
func (t *DistributeTransaction) genDisTransactionID() string {
	xid := uuid.Must(uuid.NewV4()).String()
	rand.Seed(time.Now().UnixNano())
	return defaultTransactionIDPre + xid + "-" + strconv.Itoa(rand.Intn(10000))
}

// generate a bunch transaction id
func (t *DistributeTransaction) genBunchTransactionID(index int) string {
	return defaultBunchTransactionIDPre + strconv.Itoa(index)
}

func (t *DistributeTransaction) Init(metadata transaction.Metadata) {
	t.logger.Debug("initialize tranaction component")

	// initialize distribute transaction schema, default use tcc
	if metadata.Properties["schema"] != "" {
		t.schema = metadata.Properties["schema"]
	} else {
		t.schema = defaultTransactionSchema
	}

	// initialize retryTimes for confirm and roll back action
	t.retryTimes = 1
	if retry, _ := strconv.Atoi(metadata.Properties["retryTimes"]); retry > 0 {
		t.retryTimes = retry
	}
	t.InitTransactionStateStore(metadata)
}

// when begin a distribute transaction, generate a transactionID and the number of bunchTransactionID which is defined by BunchTransactionNum, persist store these into a redis map
func (t *DistributeTransaction) Begin(beginRequest transaction.BeginTransactionRequest) (*transaction.BeginResponse, error) {
	t.logger.Debug("Begin a distribute transaction")
	if beginRequest.BunchTransactionNum <= 0 {
		return nil, fmt.Errorf("must declare a positive number of bunch transactions, but %d given", beginRequest.BunchTransactionNum)
	}
	transactionID := t.genDisTransactionID()

	bunchTransactionIDs := []string{}
	bunchTransactionStateStores := []string{}

	i := 1
	for i <= beginRequest.BunchTransactionNum {
		// allot a bunch transaction id
		bunchTransactionID := t.genBunchTransactionID(i)

		// set to a default state for nothing have happened and a empty request param
		bunchTransactionStateStore := transaction.DistributeTransactionState{}
		bunchTransactionStateStore.StatusCode = defaultState
		bunchTransactionStateStore.TryRequestParam = &transaction.TransactionTryRequestParam{}
		t.logger.Debugf("init state info for %s is %s", bunchTransactionID, t.parseStructToString(bunchTransactionStateStore))

		bunchTransactionStateStores = append(bunchTransactionStateStores, bunchTransactionID, t.parseStructToString(bunchTransactionStateStore))
		bunchTransactionIDs = append(bunchTransactionIDs, bunchTransactionID)
		i++
	}

	t.logger.Debug("transaction id is :", transactionID)

	err := t.initBunchTransactionState(transactionID, bunchTransactionStateStores)
	if err != nil {
		t.logger.Debugf("distribute transaction state store error! XID: %s and error:%s", transactionID, err)
		return nil, err
	}
	t.logger.Debug("a new distribute transaction start")
	return &transaction.BeginResponse{
		TransactionID:       transactionID,
		BunchTransactionIDs: bunchTransactionIDs,
	}, nil
}

// Try to execute bunch transaction
func (t *DistributeTransaction) Try(tryRequest transaction.BunchTransactionTryRequest) error {
	t.logger.Debug("Try to execute bunch transaction")
	if tryRequest.TransactionID == "" || tryRequest.BunchTransactionID == "" {
		t.logger.Info("distribute transaction id or bunch transaction id missing")
		return fmt.Errorf("distribute transaction id or bunch transaction id missing")
	}
	bunchTransactionStateStore, err := t.getBunchTransaction(tryRequest.TransactionID, tryRequest.BunchTransactionID)
	if err != nil {
		return fmt.Errorf("distribute transaction does't found")
	}

	if tryRequest.StatusCode == requestStatusOK {
		bunchTransactionStateStore.StatusCode = stateForTrySuccess
	} else {
		bunchTransactionStateStore.StatusCode = stateForTryFailure
	}
	bunchTransactionStateStore.TryRequestParam = tryRequest.TryRequestParam

	t.logger.Debug("bunch tranastion request param :", bunchTransactionStateStore)

	err = t.modifyBunchTransactionState(tryRequest.TransactionID, tryRequest.BunchTransactionID, t.parseStructToString(bunchTransactionStateStore))
	if err != nil {
		return fmt.Errorf("distribute transaction state store error")
	}
	t.logger.Debug(tryRequest.TransactionID, "bunch transaction state store success")
	return nil
}

// Confirm a bunch trasaction
func (t *DistributeTransaction) Confirm(confirmRequest transaction.BunchTransactionConfirmRequest) error {
	t.logger.Debug("Confirm the bunch transaction")
	if confirmRequest.TransactionID == "" || confirmRequest.BunchTransactionID == "" {
		t.logger.Debug("distribute transaction id or bunch transaction id missing")
		return fmt.Errorf("distribute transaction id or bunch transaction id missing")
	}
	bunchTransactionStateStore, err := t.getBunchTransaction(confirmRequest.TransactionID, confirmRequest.BunchTransactionID)
	if err != nil {
		return fmt.Errorf("distribute transaction state store read error")
		return err
	}
	if confirmRequest.StatusCode == requestStatusOK {
		bunchTransactionStateStore.StatusCode = stateForConfirmSuccess
	} else {
		bunchTransactionStateStore.StatusCode = stateForConfirmFailure
	}

	err = t.modifyBunchTransactionState(confirmRequest.TransactionID, confirmRequest.BunchTransactionID, t.parseStructToString(bunchTransactionStateStore))
	if err != nil {
		return fmt.Errorf("distribute transaction state store error")
	}
	t.logger.Debug(confirmRequest.TransactionID, "bunch transaction state store success")
	return nil
}

// Rollback the trasaction
func (t *DistributeTransaction) Rollback(rollbackRequest transaction.BunchTransactionRollbackRequest) error {
	t.logger.Debug("Rollback the bunch transaction")
	if rollbackRequest.TransactionID == "" || rollbackRequest.BunchTransactionID == "" {
		t.logger.Info("distribute transaction id or bunch transaction id missing")
		return fmt.Errorf("distribute transaction id or bunch transaction id missing")
	}
	bunchTransactionStateStore, err := t.getBunchTransaction(rollbackRequest.TransactionID, rollbackRequest.BunchTransactionID)
	if err != nil {
		return fmt.Errorf("distribute transaction state store read error")
		return err
	}
	if rollbackRequest.StatusCode == requestStatusOK {
		bunchTransactionStateStore.StatusCode = stateForRollbackSuccess
	} else {
		bunchTransactionStateStore.StatusCode = stateForRollbackFailure
	}

	err = t.modifyBunchTransactionState(rollbackRequest.TransactionID, rollbackRequest.BunchTransactionID, t.parseStructToString(bunchTransactionStateStore))
	if err != nil {
		return fmt.Errorf("distribute transaction state store error")
	}
	t.logger.Debug(rollbackRequest.TransactionID, "bunch transaction state store success")
	return nil
}

// get all bunch transaction state of the distribute transaction
func (t *DistributeTransaction) GetBunchTransactionState(transactionReq transaction.GetBunchTransactionsRequest) (*transaction.TransactionStateResponse, error) {
	if transactionReq.TransactionID == "" {
		t.logger.Debug("distribute transaction id missing")
		return nil, fmt.Errorf("distribute transaction id missing")
	}

	bunchTransactionState, err := t.getBunchTransactionState(transactionReq.TransactionID)
	if err != nil {
		return nil, err
	}
	return &transaction.TransactionStateResponse{
		TransactionID:          transactionReq.TransactionID,
		BunchTransactionStates: bunchTransactionState,
	}, nil
}

func (t *DistributeTransaction) GetBunchTransactions(transactionReq transaction.GetBunchTransactionsRequest) (*transaction.BunchTransactionsResponse, error) {
	if transactionReq.TransactionID == "" {
		t.logger.Debug("distribute transaction id missing")
		return nil, fmt.Errorf("distribute transaction id missing")
	}

	bunchTransactions, err := t.getBunchTransactions(transactionReq.TransactionID)
	if err != nil {
		return nil, err
	}
	return &transaction.BunchTransactionsResponse{
		TransactionID:     transactionReq.TransactionID,
		BunchTransactions: bunchTransactions,
	}, nil
}

// release transaction state store when all of the bunch transaction confirmed or roll back
func (t *DistributeTransaction) ReleaseTransactionResource(releaseRequest transaction.ReleaseTransactionRequest) error {
	if releaseRequest.TransactionID == "" {
		t.logger.Info("distribute transaction id missing")
		return fmt.Errorf("distribute transaction id missing")
	}
	err := t.releaseBunchTransactionState(releaseRequest.TransactionID)
	if err != nil {
		return err
	}
	return nil
}

func (t *DistributeTransaction) GetRetryTimes() int {
	return t.retryTimes
}

func (t *DistributeTransaction) GetTransactionSchema() string {
	return t.schema
}
