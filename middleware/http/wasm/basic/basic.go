package basic

import (
	"encoding/json"
	"path/filepath"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/components-contrib/middleware/http/wasm/basic/runtime"
	"github.com/dapr/kit/logger"
	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
)

type metaData struct {
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
		meta *metaData
		err  error
	)
	meta, err = m.getNativeetadata(metadata)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse wasm basic metadata")
	}
	path, err := filepath.Abs(meta.Path)
	if err != nil {
		return nil, errors.Wrap(err, "failed to find wasm basic file")
	}
	return func(h fasthttp.RequestHandler) fasthttp.RequestHandler {
		return func(ctx *fasthttp.RequestCtx) {
			runtime := runtime.NewWASMRuntime(meta.Runtime, path)
			if err = runtime.Run(ctx); err != nil {
				m.logger.Errorf("failed to run wasm, err: %v", err)
			}

			h(ctx)
		}
	}, nil
}

func (m *Middleware) getNativeetadata(metadata middleware.Metadata) (*metaData, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var data metaData
	err = json.Unmarshal(b, &data)
	if err != nil {
		return nil, err
	}

	return &data, nil
}
