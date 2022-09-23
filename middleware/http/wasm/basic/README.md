## Basic WebAssembly Middleware

WebAssembly is a way to safely run code compiled in other languages. Runtimes
execute WebAssembly Modules (Wasm), which are most often binaries with a `.wasm`
extension.

This component allows you to rewrite a request URI with custom logic compiled
to a Wasm using the [waPC protocol][1].

Please see the [documentation][2] for general configuration.

### Generating Wasm

To compile your wasm, you must compile source using a wapc-go guest SDK such as
[TinyGo][3]. You can also make a copy of [hello.go](./example/example.go) and
replace the function `rewrite` with your custom logic.

If using TinyGo, compile like so and set the `path` attribute to the output:
```bash
tinygo build -o example.wasm -scheduler=none --no-debug -target=wasi example.go`
```

### Notes

* This is an alpha feature, so configuration is subject to change.
* This module implements the host side of the waPC protocol using [wapc-go][4].
* This uses [wazero][5] for the WebAssembly runtime as it has no dependencies,
  nor relies on CGO. This allows installation without shared libraries.
* Many WebAssembly compilers leave memory unbounded and/or set to 16MB. Do not
  set a large pool size without considering memory amplification.

[1]: https://wapc.io/docs/spec/
[2]: https://github.com/dapr/docs/blob/v1.8/daprdocs/content/en/reference/components-reference/supported-middleware/middleware-wasm.md
[3]: https://github.com/wapc/wapc-guest-tinygo
[4]: https://github.com/wapc/wapc-go
[5]: https://wazero.io
 