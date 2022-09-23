package main

import "github.com/wapc/wapc-guest-tinygo"

func main() {
	wapc.RegisterFunctions(wapc.Functions{"rewrite": rewrite})
}

// rewrite returns a new URI if necessary.
func rewrite(requestURI []byte) ([]byte, error) {
	if string(requestURI) == "/v1.0/hi" {
		return []byte("/v1.0/hello"), nil
	}
	return requestURI, nil
}
