package transaction

import (
	fasthttp "github.com/valyala/fasthttp"
)

// The input params for begin a distribute transaction
type BeginTransactionRequest struct {
	BunchTransactionNum int `json:"bunchTransactionNum"`
}

// The input params for try to lock a bunch transaction resource
type TryTransactionRequest struct {
	TargetID         string                  `json:"targetID"`
	InvokeMethodName string                  `json:"invokeMethodName"`
	Verb             string                  `json:"verb"`
	QueryArgs        string                  `json:"queryArgs"`
	Data             []byte                  `json:"data"`
	ContentType      string                  `json:"contentType"`
	Header           *fasthttp.RequestHeader `json:"header"`
}
