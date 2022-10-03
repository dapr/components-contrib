// Package main ensures tests can prove logging or stdio isn't missed, both
// during initialization (main) and request (rewrite).
package main

import (
	"fmt"
	"github.com/http-wasm/http-wasm-guest-tinygo/handler"
	"os"
)

func main() {
	fmt.Fprintln(os.Stdout, "main Stdout")
	fmt.Fprintln(os.Stderr, "main Stderr")
	handler.Log("main ConsoleLog")
	handler.HandleFn = log
}

var requestCount int

func log() {
	fmt.Fprintf(os.Stdout, "request[%d] Stdout\n", requestCount)
	fmt.Fprintf(os.Stderr, "request[%d] Stderr\n", requestCount)
	handler.Log(fmt.Sprintf("request[%d] ConsoleLog", requestCount))
	requestCount++
	handler.Next()
}
