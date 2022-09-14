package basic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/valyala/fasthttp"
	"github.com/wapc/wapc-go"
	"github.com/wapc/wapc-go/engines/wazero"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

// ctx substitutes for context propagation until middleware APIs support it.
var ctx = context.Background()

// middlewareMetadata includes configuration used for the WebAssembly handler.
// Detailed notes are in README.md for visibility.
//
// Note: When changing this, you must update the docs with summary comments per
// field.
// https://github.com/dapr/docs/blob/v1.8/daprdocs/content/en/reference/components-reference/supported-middleware/middleware-wasm.md
type middlewareMetadata struct {
	// Path is where to load a `%.wasm` file that implements the guest side of
	// the waPC protocol. No default.
	Path string `json:"path"`

	// PoolSize determines the amount of modules at the given path to load, per
	// request handler. Default: 10
	PoolSize poolSizeJSON `json:"poolSize"`

	// guest is WebAssembly binary implementing the waPC guest, loaded from Path.
	guest []byte
}

// poolSizeJSON is needed because go cannot unmarshal an integer from a string.
type poolSizeJSON uint32

// UnmarshalJSON allows decoding of a quoted uint32
func (s *poolSizeJSON) UnmarshalJSON(b []byte) error {
	var n json.Number
	if err := json.Unmarshal(b, &n); err != nil {
		return fmt.Errorf("invalid poolSize: %w", err)
	}
	if i, err := strconv.ParseUint(string(n), 10, 32); err != nil {
		return fmt.Errorf("invalid poolSize: %w", err)
	} else {
		*s = poolSizeJSON(i)
		return nil
	}
}

// wapcMiddleware is a wasm basic middleware.
type wapcMiddleware struct {
	logger logger.Logger
}

// NewMiddleware returns a new wasm basic middleware.
func NewMiddleware(logger logger.Logger) middleware.Middleware {
	return &wapcMiddleware{logger: logger}
}

// GetHandler returns the HTTP handler provided by wasm basic middleware.
func (m *wapcMiddleware) GetHandler(metadata middleware.Metadata) (func(h fasthttp.RequestHandler) fasthttp.RequestHandler, error) {
	rh, err := m.getHandler(metadata)
	if err != nil {
		return nil, err
	}
	runtime.SetFinalizer(rh, (*wapcRequestHandler).Close)
	return rh.requestHandler, nil
}

// getHandler is extracted for unit testing.
func (m *wapcMiddleware) getHandler(metadata middleware.Metadata) (*wapcRequestHandler, error) {
	meta, err := m.getMetadata(metadata)
	if err != nil {
		return nil, fmt.Errorf("wasm basic: failed to parse metadata: %w", err)
	}

	var stdout, stderr bytes.Buffer
	config := &wapc.ModuleConfig{
		Logger: m.log,   // waPC messages go here
		Stdout: &stdout, // reset per request
		Stderr: &stderr,
	}

	// This is a simple case, so the binary does not need any callbacks.
	mod, err := wazero.Engine().New(ctx, wapc.NoOpHostCallHandler, meta.guest, config)
	if err != nil {
		return nil, fmt.Errorf("wasm basic: error compiling wasm at %s: %w", meta.Path, err)
	}

	// WebAssembly modules are not goroutine safe (because they have no atomics
	// to implement garbage collection safely). Hence, we need a pool.
	pool, err := wapc.NewPool(ctx, mod, uint64(meta.PoolSize))
	if err != nil {
		return nil, fmt.Errorf("error creating module pool from wasm at %s: %w", meta.Path, err)
	}

	return &wapcRequestHandler{mod: mod, logger: m.logger, stdout: &stdout, stderr: &stderr, pool: pool}, nil
}

// log implements wapc.Logger.
func (m *wapcMiddleware) log(msg string) {
	m.logger.Info(msg)
}

func (m *wapcMiddleware) getMetadata(metadata middleware.Metadata) (*middlewareMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var data middlewareMetadata
	err = json.Unmarshal(b, &data)
	if err != nil {
		return nil, err
	}

	if data.Path == "" {
		return nil, errors.New("missing path")
	}

	data.guest, err = os.ReadFile(data.Path)
	if err != nil {
		return nil, fmt.Errorf("error reading path: %w", err)
	}

	if data.PoolSize == 0 {
		data.PoolSize = 10 // Default
	}

	return &data, nil
}

type wapcRequestHandler struct {
	mod            wapc.Module
	logger         logger.Logger
	stdout, stderr *bytes.Buffer
	pool           *wapc.Pool
}

func (rh *wapcRequestHandler) requestHandler(h fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		instance, err := rh.pool.Get(1 * time.Second)
		if err != nil {
			ctx.Error("wasm pool busy", fasthttp.StatusInternalServerError)
			return
		}
		defer func() {
			rh.stdout.Reset()
			rh.stderr.Reset()
			_ = rh.pool.Return(instance)
		}()

		err = rh.handle(ctx, instance)
		if stdout := rh.stdout.String(); len(stdout) > 0 {
			rh.logger.Debugf("wasm stdout: %s", stdout)
		}
		if stderr := rh.stderr.String(); len(stderr) > 0 {
			rh.logger.Debugf("wasm stderr: %s", stderr)
		}
		if err != nil {
			ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		} else {
			h(ctx)
		}
	}
}

// handle is like fasthttp.RequestHandler, except it accepts a waPC instance
// and returns an error.
func (rh *wapcRequestHandler) handle(ctx *fasthttp.RequestCtx, instance wapc.Instance) error {
	if uri, err := instance.Invoke(ctx, "rewrite", ctx.RequestURI()); err != nil {
		return err
	} else {
		ctx.Request.SetRequestURIBytes(uri)
	}
	return nil
}

// Close implements io.Closer
func (rh *wapcRequestHandler) Close() error {
	// TODO: we have to use a finalizer as there's no way in dapr to close middleware, yet.
	// See https://github.com/dapr/dapr/pull/3088
	runtime.SetFinalizer(rh, nil)

	rh.pool.Close(ctx)
	return rh.mod.Close(ctx)
}
