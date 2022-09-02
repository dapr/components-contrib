package transaction

import (
	fasthttp "github.com/valyala/fasthttp"
)

// The input params for begin a distribute transaction
type BeginTransactionRequest struct {
	BunchTransactionNum int `json:"bunchTransactionNum"`
}

type SaveBunchTransactionRequest struct {
	TransactionID                string                   `json:"transactionID"`
	BunchTransactionID           string                   `json:"bunchTransactionID"`
	StatusCode                   int                      `json:"statusCode"`
	BunchTransactionRequestParam *TransactionRequestParam `json:"bunchTransactionRequestParam"`
}

// The request params of a bunch transaction
type TransactionRequestParam struct {
	Type             string                  `json:"type"`
	TargetID         string                  `json:"targetID"`
	InvokeMethodName string                  `json:"invokeMethodName"`
	Verb             string                  `json:"verb"`
	QueryArgs        string                  `json:"queryArgs"`
	Data             []byte                  `json:"data"`
	ContentType      string                  `json:"contentType"`
	Header           *fasthttp.RequestHeader `json:"header"`
}

type GetBunchTransactionsRequest struct {
	TransactionID string `json:"transactionID"`
}

type BunchTransactionCommitRequest struct {
	TransactionID      string `json:"transactionID"`
	BunchTransactionID string `json:"bunchTransactionID"`
	StatusCode         int    `json:"statusCode"`
}

type BunchTransactionRollbackRequest struct {
	TransactionID      string `json:"transactionID"`
	BunchTransactionID string `json:"bunchTransactionID"`
	StatusCode         int    `json:"statusCode"`
}

type ReleaseTransactionRequest struct {
	TransactionID string `json:"transactionID"`
}

type DistributeTransactionState struct {
	StatusCode                   int                      `json:"statusCode"`
	BunchTransactionRequestParam *TransactionRequestParam `json:"bunchTransactionRequestParam"`
}
