## WebAssembly Middleware

This component lets you manipulate an incoming request or serve a response with custom logic compiled using the [htp-wasm](https://http-wasm.io/) Application Binary Interface (ABI). The `handle_request` function receives an incoming request and can manipulate it or serve a response as necessary.

Please see the [documentation](https://github.com/dapr/docs/blob/v1.9/daprdocs/content/en/reference/components-reference/supported-middleware/middleware-wasm.md) for general configuration.

### Generating Wasm

To compile your wasm, you must compile source using an SDK such as [http-wasm-guest-tinygo](https://github.com/http-wasm/http-wasm-guest-tinygo). You can also make a copy of [hello.go](./example/example.go) and replace the `handler.HandleFn` function with your custom logic.

If using TinyGo, compile like so and set the `path` attribute to the output:
```bash
tinygo build -o router.wasm -scheduler=none --no-debug -target=wasi router.go`
```

### Notes

* This is an alpha feature, so configuration is subject to change.
* This module implements the host side of the http-wasm handler protocol.
* This uses [wazero](https://wazero.io) for the WebAssembly runtime as it has no dependencies,
  nor relies on CGO. This allows installation without shared libraries.
* Many WebAssembly compilers leave memory unbounded and/or set to 16MB. To
  avoid resource exhaustion, assign [concurrency controls](https://docs.dapr.io/operations/configuration/control-concurrency/).
