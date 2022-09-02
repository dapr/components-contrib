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
	stateForRequestSuccess       = 10
	stateForRequestFailure       = 1
	stateForCommitSuccess        = 20
	stateForCommitFailure        = 2
	stateForRollbackSuccess      = 30
	stateForRollbackFailure      = 3
	requestStatusOK              = 1
	defaultTransactionScheme     = "tcc"
)

type DistributeTransaction struct {
	logger         logger.Logger
	client         redis.UniversalClient
	clientSettings *rediscomponent.Settings
	metadata       rediscomponent.Metadata
	cancel         context.CancelFunc
	ctx            context.Context
	duration       int
	scheme         string
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
	// update ttl for the map, it doesn't mater get a failed result as it will be deleted when the transaction committed or rollback.
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
func (t *DistributeTransaction) getBunchTransactionState(transactionID string) (map[string]string, error) {
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

	bunchTransactionState := make(map[string]string)
	for bunchTransactionID, stateInfo := range bunchTransactionStatePersit {
		t.logger.Debugf("state of bunch transaction: %s is %s ", bunchTransactionID, stateInfo)
		bunchTransaction := transaction.DistributeTransactionState{}
		err = t.parseStringToStruct(stateInfo, &bunchTransaction)
		if err != nil {
			return nil, err
		}
		t.logger.Debug("state parse res: ", bunchTransaction)
		t.logger.Debugf("state result is: %d", bunchTransaction.StatusCode)
		var stateInfo string
		switch bunchTransaction.StatusCode {
		case defaultState:
			stateInfo = "begin"
		case stateForRequestSuccess:
			stateInfo = "request-success"
		case stateForRequestFailure:
			stateInfo = "request-failure"
		case stateForCommitSuccess:
			stateInfo = "commit-success "
		case stateForCommitFailure:
			stateInfo = "commit-failure "
		case stateForRollbackSuccess:
			stateInfo = "rollback-success"
		case stateForRollbackFailure:
			stateInfo = "rollback-failure"
		}
		bunchTransactionState[bunchTransactionID] = stateInfo
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
	res := t.client.Del(t.ctx, transactionID)
	if res.Err() != nil {
		t.logger.Debug("release transaction from persistent store error : ", res.Err())
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

	// initialize distribute transaction scheme, default use tcc
	if metadata.Properties["scheme"] != "" {
		t.scheme = metadata.Properties["scheme"]
	} else {
		t.scheme = defaultTransactionScheme
	}

	// initialize retryTimes for commit and roll back action
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
		bunchTransactionStateStore.BunchTransactionRequestParam = &transaction.TransactionRequestParam{}
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

// store the state of bunch transaction
func (t *DistributeTransaction) SaveBunchTransactionState(saveRequest transaction.SaveBunchTransactionRequest) error {
	t.logger.Debug("Try to execute bunch transaction")
	if saveRequest.TransactionID == "" || saveRequest.BunchTransactionID == "" {
		t.logger.Info("distribute transaction id or bunch transaction id missing")
		return fmt.Errorf("distribute transaction id or bunch transaction id missing")
	}
	bunchTransactionStateStore, err := t.getBunchTransaction(saveRequest.TransactionID, saveRequest.BunchTransactionID)
	if err != nil {
		return fmt.Errorf("distribute transaction does't found")
	}

	if saveRequest.StatusCode == requestStatusOK {
		bunchTransactionStateStore.StatusCode = stateForRequestSuccess
	} else {
		bunchTransactionStateStore.StatusCode = stateForRequestFailure
	}
	bunchTransactionStateStore.BunchTransactionRequestParam = saveRequest.BunchTransactionRequestParam

	t.logger.Debug("bunch tranastion request param :", bunchTransactionStateStore)

	err = t.modifyBunchTransactionState(saveRequest.TransactionID, saveRequest.BunchTransactionID, t.parseStructToString(bunchTransactionStateStore))
	if err != nil {
		return fmt.Errorf("distribute transaction state store error")
	}
	t.logger.Debug(saveRequest.TransactionID, "bunch transaction state store success")
	return nil
}

// Commit a bunch trasaction
func (t *DistributeTransaction) Commit(commitRequest transaction.BunchTransactionCommitRequest) error {
	t.logger.Debug("Commit the bunch transaction")
	if commitRequest.TransactionID == "" || commitRequest.BunchTransactionID == "" {
		t.logger.Debug("distribute transaction id or bunch transaction id missing")
		return fmt.Errorf("distribute transaction id or bunch transaction id missing")
	}
	bunchTransactionStateStore, err := t.getBunchTransaction(commitRequest.TransactionID, commitRequest.BunchTransactionID)
	if err != nil {
		return fmt.Errorf("distribute transaction state store read error")
	}
	if commitRequest.StatusCode == requestStatusOK {
		bunchTransactionStateStore.StatusCode = stateForCommitSuccess
	} else {
		bunchTransactionStateStore.StatusCode = stateForCommitFailure
	}

	err = t.modifyBunchTransactionState(commitRequest.TransactionID, commitRequest.BunchTransactionID, t.parseStructToString(bunchTransactionStateStore))
	if err != nil {
		return fmt.Errorf("distribute transaction state store error")
	}
	t.logger.Debug(commitRequest.TransactionID, "bunch transaction state store success")
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

// release transaction state store when all of the bunch transaction committed or rollback
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

func (t *DistributeTransaction) GetTransactionScheme() string {
	return t.scheme
}
