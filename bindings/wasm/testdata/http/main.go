package main

// Building main.wasm:
// go get github.com/dev-wasm/dev-wasm-go/http/client
// tinygo build -target wasi -o main.wasm main.go

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	wasiclient "github.com/dev-wasm/dev-wasm-go/http/client"
)

func printResponse(r *http.Response) {
	fmt.Printf("Status: %d\n", r.StatusCode)
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err.Error())
	}
	fmt.Printf("Body: \n%s\n", body)
}

func main() {
	client := http.Client{
		Transport: wasiclient.WasiRoundTripper{},
	}
	res, err := client.Get(os.Args[1])
	if err != nil {
		panic(err.Error())
	}
	defer res.Body.Close()
	printResponse(res)
}
