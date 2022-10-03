package basic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	dapr "github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
	wasm "github.com/http-wasm/http-wasm-host-go"
	wasmfast "github.com/http-wasm/http-wasm-host-go/handler/fasthttp"
	"github.com/tetratelabs/wazero"
	"github.com/valyala/fasthttp"
	"os"
	"runtime"
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
	// the handler protocol. No default.
	Path string `json:"path"`

	// guest is WebAssembly binary implementing the handler guest, loaded from Path.
	guest []byte
}

// middleware is a wasm basic middleware.
type middleware struct {
	logger logger.Logger
}

// NewMiddleware returns a new wasm basic middleware.
func NewMiddleware(logger logger.Logger) dapr.Middleware {
	return &middleware{logger: logger}
}

// GetHandler returns the HTTP handler provided by wasm basic middleware.
func (m *middleware) GetHandler(metadata dapr.Metadata) (func(h fasthttp.RequestHandler) fasthttp.RequestHandler, error) {
	rh, err := m.getHandler(metadata)
	if err != nil {
		return nil, err
	}
	runtime.SetFinalizer(rh, (*requestHandler).Close)
	return rh.requestHandler, nil
}

// getHandler is extracted for unit testing.
func (m *middleware) getHandler(metadata dapr.Metadata) (*requestHandler, error) {
	meta, err := m.getMetadata(metadata)
	if err != nil {
		return nil, fmt.Errorf("wasm basic: failed to parse metadata: %w", err)
	}

	var stdout, stderr bytes.Buffer
	mw, err := wasmfast.NewMiddleware(ctx, meta.guest,
		wasm.Logger(m.log),
		wasm.GuestConfig(wazero.NewModuleConfig().
			WithStdout(&stdout).  // reset per request
			WithStderr(&stderr)), // reset per request
	)
	if err != nil {
		return nil, err
	}

	// TODO: meta.PoolSize

	return &requestHandler{mw: mw, logger: m.logger, stdout: &stdout, stderr: &stderr}, nil
}

// log implements wasm.Logger.
func (m *middleware) log(_ context.Context, message string) {
	m.logger.Info(message)
}

func (m *middleware) getMetadata(metadata dapr.Metadata) (*middlewareMetadata, error) {
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

	return &data, nil
}

type requestHandler struct {
	mw             wasmfast.Middleware
	logger         logger.Logger
	stdout, stderr *bytes.Buffer
}

func (rh *requestHandler) requestHandler(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		h, err := rh.mw.NewHandler(ctx, next)
		if err != nil {
			ctx.Error("error creating handler", fasthttp.StatusInternalServerError)
			return
		}
		defer func() {
			rh.stdout.Reset()
			rh.stderr.Reset()
			h.Close(ctx)
		}()

		h.Handle(ctx)

		if stdout := rh.stdout.String(); len(stdout) > 0 {
			rh.logger.Debugf("wasm stdout: %s", stdout)
		}
		if stderr := rh.stderr.String(); len(stderr) > 0 {
			rh.logger.Debugf("wasm stderr: %s", stderr)
		}
	}
}

// Close implements io.Closer
func (rh *requestHandler) Close() error {
	// TODO: we have to use a finalizer as there's no way in dapr to close middleware, yet.
	// See https://github.com/dapr/dapr/pull/3088
	runtime.SetFinalizer(rh, nil)

	return rh.mw.Close(ctx)
}
