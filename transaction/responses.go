package transaction

type BeginResponse struct {
	TransactionID       string   `json:"transactionID"`
	BunchTransactionIDs []string `json:"bunchTransactionIDs"`
}

type TransactionStateResponse struct {
	TransactionID          string         `json:"transactionID"`
	BunchTransactionStates map[string]int `json:"bunchTransactionStates"`
}

type BunchTransactionsResponse struct {
	TransactionID     string                                `json:"transactionID"`
	BunchTransactions map[string]DistributeTransactionState `json:"bunchTransactions"`
}
