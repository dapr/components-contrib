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

// Package main ensures tests can prove logging or stdio isn't missed, both
// during initialization (main) and request (rewrite).
package main

import (
	"fmt"
	"os"

	"github.com/http-wasm/http-wasm-guest-tinygo/handler"
	"github.com/http-wasm/http-wasm-guest-tinygo/handler/api"
)

func main() {
	fmt.Fprintln(os.Stdout, "main Stdout")
	fmt.Fprintln(os.Stderr, "main Stderr")
	handler.Host.Log(api.LogLevelInfo, "main ConsoleLog")
	handler.HandleRequestFn = log
}

var requestCount int

func log(api.Request, api.Response) (next bool, reqCtx uint32) {
	fmt.Fprintf(os.Stdout, "request[%d] Stdout\n", requestCount)
	fmt.Fprintf(os.Stderr, "request[%d] Stderr\n", requestCount)
	handler.Host.Log(api.LogLevelInfo, fmt.Sprintf("request[%d] ConsoleLog", requestCount))
	requestCount++
	next = true
	return
}
