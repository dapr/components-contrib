package transaction

import (
	fasthttp "github.com/valyala/fasthttp"
)

// The input params for begin a distribute transaction
type BeginTransactionRequest struct {
	BunchTransactionNum int `json:"bunchTransactionNum"`
}

type BunchTransactionTryRequest struct {
	TransactionId      string                     `json:"transactionId"`
	BunchTransactionId string                     `json:"bunchTransactionId"`
	StatusCode         int                        `json:"statusCode"`
	TryRequestParam    TransactionTryRequestParam `json:"tryRequestParam"`
}

// The request params of a bunch transaction
type TransactionTryRequestParam struct {
	TargetID         string                  `json:"targetID"`
	InvokeMethodName string                  `json:"invokeMethodName"`
	Verb             string                  `json:"verb"`
	QueryArgs        string                  `json:"queryArgs"`
	Data             []byte                  `json:"data"`
	ContentType      string                  `json:"contentType"`
	Header           *fasthttp.RequestHeader `json:"header"`
}

type GetBunchTransactionsRequest struct {
	TransactionId string `json:"transactionId"`
}

type BunchTransactionConfirmRequest struct {
	TransactionId      string `json:"transactionId"`
	BunchTransactionId string `json:"bunchTransactionId"`
	StatusCode         int    `json:"statusCode"`
}

type BunchTransactionRollBackRequest struct {
	TransactionId      string `json:"transactionId"`
	BunchTransactionId string `json:"bunchTransactionId"`
	StatusCode         int    `json:"statusCode"`
}
