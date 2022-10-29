package main

import (
	"github.com/http-wasm/http-wasm-guest-tinygo/handler"
	"github.com/http-wasm/http-wasm-guest-tinygo/handler/api"
)

func main() {
	handler.HandleRequestFn = handleRequest
}

// handle rewrites the request URI before dispatching to the next handler.
//
// Note: This is not a redirect, rather in-process routing.
func handleRequest(req api.Request, resp api.Response) (next bool, reqCtx uint32) {
	if req.GetURI() == "/v1.0/hi?name=panda" {
		req.SetURI("/v1.0/hello?name=teddy")
	}
	next = true
	return
}
