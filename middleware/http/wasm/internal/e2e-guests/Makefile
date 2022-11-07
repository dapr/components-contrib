tinygo_sources := $(wildcard */*.go)
build: $(tinygo_sources)
	@echo "Building End-to-End Guest Wasm"
	@for f in $^; do \
	    tinygo build -o $$(echo $$f | sed -e 's/\.go/\.wasm/') -scheduler=none --no-debug -target=wasi $$f; \
	done
