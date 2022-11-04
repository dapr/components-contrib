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
