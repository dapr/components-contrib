module github.com/dapr/components-contrib/bindings/wasm/testdata/http

go 1.20

// This is the last version building with
// stealthrocket/wasi-go host ABI.
// The guest is revlocked to TinyGo 0.28.1
require github.com/dev-wasm/dev-wasm-go/http v0.0.0-20230803142009-0dee5e3d3722
