package main

import "github.com/http-wasm/http-wasm-guest-tinygo/handler"

func main() {
	handler.HandleFn = rewrite
}

func rewrite() {
	if handler.GetPath() == "/v1.0/hi" {
		handler.SetPath("/v1.0/hello")
	}
	handler.Next()
}
