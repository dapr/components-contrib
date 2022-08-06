package transaction

type BeginResponse struct {
	TransactionId        string                 `json:"transactionId"`
	BranchTransactionIds map[string]interface{} `json:"branchTransactionIds"`
}
