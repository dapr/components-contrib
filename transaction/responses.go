package transaction

type BeginResponse struct {
	TransactionId       string   `json:"transactionId"`
	BunchTransactionIds []string `json:"bunchTransactionIds"`
}

type TransactionStateResponse struct {
	TransactionId          string         `json:"transactionId"`
	BunchTransactionStates map[string]int `json:"bunchTransactionStates"`
}
