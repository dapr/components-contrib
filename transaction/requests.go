package transaction

import (
	fasthttp "github.com/valyala/fasthttp"
)

// The input params for begin a distribute transaction
type BeginTransactionRequest struct {
	BunchTransactionNum int `json:"bunchTransactionNum"`
}

type BunchTransactionTryRequest struct {
	TransactionID      string                      `json:"transactionID"`
	BunchTransactionID string                      `json:"bunchTransactionID"`
	StatusCode         int                         `json:"statusCode"`
	TryRequestParam    *TransactionTryRequestParam `json:"tryRequestParam"`
}

// The request params of a bunch transaction
type TransactionTryRequestParam struct {
	Type             string                  `json:"type"`
	TargetID         string                  `json:"targetID"`
	InvokeMethodName string                  `json:"invokeMethodName"`
	Verb             string                  `json:"verb"`
	QueryArgs        string                  `json:"queryArgs"`
	Data             []byte                  `json:"data"`
	ContentType      string                  `json:"contentType"`
	Header           *fasthttp.RequestHeader `json:"header"`
	ActorType        string                  `json:"actorType"`
	ActorID          string                  `json:"actorID"`
}

type GetBunchTransactionsRequest struct {
	TransactionID string `json:"transactionID"`
}

type BunchTransactionConfirmRequest struct {
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
	StatusCode      int                         `json:"statusCode"`
	TryRequestParam *TransactionTryRequestParam `json:"tryRequestParam"`
}
