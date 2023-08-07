/*
Copyright 2023 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
