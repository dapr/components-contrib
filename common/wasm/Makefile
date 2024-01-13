%/main.wasm: %/main.go
	@(cd $(@D); tinygo build -o main.wasm -scheduler=none --no-debug -target=wasi main.go)

.PHONY: build-tinygo
build-tinygo: testdata/args/main.wasm testdata/strict/main.wasm

.PHONY: testdata
testdata:
	@$(MAKE) build-tinygo
