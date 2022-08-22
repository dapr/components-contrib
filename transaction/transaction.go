package transaction

type Transaction interface {
	// Init this component.
	Init(metadata Metadata)

	// Begin a distribute transaction
	Begin(request BeginTransactionRequest) (*BeginResponse, error)

	// Commit a distribute transaction
	Commit(commitRequest BunchTransactionCommitRequest) error

	// rooback a distribute transaction
	Rollback(rollBackRequest BunchTransactionRollbackRequest) error

	// store the state of bunch transaction
	SaveBunchTransactionState(upRequest SaveBunchTransactionRequest) error

	// get all bunch transaction state of the distribute transaction
	GetBunchTransactionState(req GetBunchTransactionsRequest) (*TransactionStateResponse, error)

	GetBunchTransactions(transactionReq GetBunchTransactionsRequest) (*BunchTransactionsResponse, error)

	ReleaseTransactionResource(releaseRequest ReleaseTransactionRequest) error

	GetRetryTimes() int

	GetTransactionSchema() string
}
