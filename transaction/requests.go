package transaction

// The input params for begin a distribute transaction
type BeginTransactionRequest struct {
	BanchTransactionNum int `json:"banchTransactionNum"`
}
