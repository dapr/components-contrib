package basic

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/wasi"
	"github.com/valyala/fasthttp"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

type middlewareMetadata struct {
	Path    string `json:"path"`
	Runtime string `json:"runtime"`
}

// Middleware is an wasm basic middleware.
type Middleware struct {
	logger logger.Logger
}

// NewMiddleware returns a new wasm basic middleware.
func NewMiddleware(logger logger.Logger) *Middleware {
	return &Middleware{logger: logger}
}

// GetHandler returns the HTTP handler provided by wasm basic middleware.
func (m *Middleware) GetHandler(metadata middleware.Metadata) (func(h fasthttp.RequestHandler) fasthttp.RequestHandler, error) {
	var (
		meta *middlewareMetadata
		err  error
	)
	meta, err = m.getNativeMetadata(metadata)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse wasm basic metadata")
	}
	if meta.Runtime != "wazero" {
		return nil, errors.Wrap(err, "only support wazero runtime")
	}
	path, err := filepath.Abs(meta.Path)
	if err != nil {
		return nil, errors.Wrap(err, "failed to find wasm basic file")
	}
	return func(h fasthttp.RequestHandler) fasthttp.RequestHandler {
		return func(ctx *fasthttp.RequestCtx) {
			// Create a new WebAssembly Runtime.
			r := wazero.NewRuntime()

			// TinyGo needs WASI to implement functions such as panic.
			wm, err := wasi.InstantiateSnapshotPreview1(ctx, r)
			if err != nil {
				log.Fatal(err)
			}
			defer wm.Close(ctx)

			wasmByte, err := os.ReadFile(path)
			if err != nil {
				log.Fatalf("wasm file %v", err)
			}

			mod, err := r.InstantiateModuleFromCode(ctx, wasmByte)
			if err != nil {
				log.Fatal(err)
			}
			defer mod.Close(ctx)

			// These are undocumented, but exported. See tinygo-org/tinygo#2788
			malloc := mod.ExportedFunction("malloc")
			free := mod.ExportedFunction("free")

			uriFunc := mod.ExportedFunction("run")
			if uriFunc == nil {
				log.Fatal(err)
			}

			uri := ctx.RequestURI()
			uriLength := uint64(len(uri))

			// Instead of an arbitrary memory offset, use TinyGo's allocator. Notice
			// there is nothing string-specific in this allocation function. The same
			// function could be used to pass binary serialized data to Wasm.
			results, err := malloc.Call(ctx, uriLength)
			if err != nil {
				log.Fatal(err)
			}
			uriPtr := results[0]
			// This pointer is managed by TinyGo, but TinyGo is unaware of external usage.
			// So, we have to free it when finished
			defer free.Call(ctx, uriPtr)

			// The pointer is a linear memory offset, which is where we write the value.
			if !mod.Memory().Write(ctx, uint32(uriPtr), uri) {
				log.Fatalf("Memory.Write(%d, %d) out of range of memory size %d",
					uriPtr, uriLength, mod.Memory().Size(ctx))
			}

			// Now, we can call "uriFunc", which reads the string we wrote to memory!
			ptrSize, err := uriFunc.Call(ctx, uriPtr, uriLength)
			// if err != nil {
			if err != nil {
				log.Fatal(err)
			}

			// Note: This pointer is still owned by TinyGo, so don't try to free it!
			retPtr := uint32(ptrSize[0] >> 32)
			retSize := uint32(ptrSize[0])
			// The pointer is a linear memory offset, which is where we write the name.
			if bytes, ok := mod.Memory().Read(ctx, retPtr, retSize); !ok {
				log.Fatalf("Memory.Read(%d, %d) out of range of memory size %d",
					retPtr, retSize, mod.Memory().Size(ctx))
			} else {
				ctx.Request.SetRequestURIBytes(bytes)
			}

			h(ctx)
		}
	}, nil
}

func (m *Middleware) getNativeMetadata(metadata middleware.Metadata) (*middlewareMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var data middlewareMetadata
	err = json.Unmarshal(b, &data)
	if err != nil {
		return nil, err
	}

	return &data, nil
}
