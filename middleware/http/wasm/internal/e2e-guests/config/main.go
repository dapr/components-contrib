// Package main ensures tests can prove logging or stdio isn't missed, both
// during initialization (main) and request (rewrite).
package main

import (
	"github.com/http-wasm/http-wasm-guest-tinygo/handler"
	"github.com/http-wasm/http-wasm-guest-tinygo/handler/api"
)

func main() {
	handler.Host.Log(api.LogLevelInfo, string(handler.Host.GetConfig()))
}
