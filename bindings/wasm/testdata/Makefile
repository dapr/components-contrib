tinygo_sources := ./args/main.go ./example/main.go ./loop/main.go
.PHONY: build
build: $(tinygo_sources)
	@for f in $^; do \
	    tinygo build -o $$(echo $$f | sed -e 's/\.go/\.wasm/') -scheduler=none --no-debug --target=wasi $$f; \
	done
