package transaction

type BeginResponse struct {
	TransactionId       string   `json:"transactionId"`
	BunchTransactionIds []string `json:"bunchTransactionIds"`
}
