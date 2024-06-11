tinygo_sources := ./args/main.go ./example/main.go ./loop/main.go
.PHONY: build
build: $(tinygo_sources)
	@for f in $^; do \
		cd $$(dirname $$f); \
		fname=$$(basename $$f); \
	    tinygo build -o $$(echo $$fname | sed -e 's/\.go/\.wasm/') -scheduler=none --no-debug --target=wasi $$fname; \
		cd ..; \
	done
	@# http/main.go is compiled seprately because it requires a scheduler.
	@(cd ./http && tinygo build -o main.wasm --no-debug --target=wasi ./main.go)
