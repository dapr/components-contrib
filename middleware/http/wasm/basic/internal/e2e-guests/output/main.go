// Package main ensures tests can prove logging or stdio isn't missed, both
// during initialization (main) and request (rewrite).
package main

import (
	"fmt"
	"os"

	"github.com/wapc/wapc-guest-tinygo"
)

func main() {
	fmt.Fprintln(os.Stdout, "main Stdout")
	fmt.Fprintln(os.Stderr, "main Stderr")
	wapc.ConsoleLog("main ConsoleLog")
	wapc.RegisterFunctions(wapc.Functions{"rewrite": rewrite})
}

var requestCount int

func rewrite(requestURI []byte) ([]byte, error) {
	fmt.Fprintf(os.Stdout, "request[%d] Stdout\n", requestCount)
	fmt.Fprintf(os.Stderr, "request[%d] Stderr\n", requestCount)
	wapc.ConsoleLog(fmt.Sprintf("request[%d] ConsoleLog", requestCount))
	requestCount++
	return requestURI, nil
}
