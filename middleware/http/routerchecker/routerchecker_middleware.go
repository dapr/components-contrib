package routerchecker

import (
	"encoding/json"
	"regexp"

	"github.com/valyala/fasthttp"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

// Metadata is the routerchecker middleware config.
type Metadata struct {
	Rule string `json:"rule"`
}

// NewRouterCheckerMiddleware returns a new routerchecker middleware.
func NewMiddleware(logger logger.Logger) *Middleware {
	return &Middleware{logger: logger}
}

// Middleware is an routerchecker middleware.
type Middleware struct {
	logger logger.Logger
}

// GetHandler retruns the HTTP handler provided by the middleware.
func (m *Middleware) GetHandler(metadata middleware.Metadata) (
	func(h fasthttp.RequestHandler) fasthttp.RequestHandler, error) {
	meta, err := m.getNativeMetadata(metadata)
	if err != nil {
		return nil, err
	}

	return func(h fasthttp.RequestHandler) fasthttp.RequestHandler {
		return func(ctx *fasthttp.RequestCtx) {
			isPass, err := regexp.MatchString(meta.Rule, string(ctx.RequestURI()))
			if err != nil {
				m.logger.Error("regexp match failed", err.Error())
				ctx.Error("regexp match failed", fasthttp.StatusBadRequest)

				return
			}
			if !isPass {
				ctx.Error("invalid router", fasthttp.StatusBadRequest)

				return
			}
			h(ctx)
		}
	}, nil
}

func (m *Middleware) getNativeMetadata(metadata middleware.Metadata) (*Metadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var middlewareMetadata Metadata
	err = json.Unmarshal(b, &middlewareMetadata)
	if err != nil {
		return nil, err
	}

	return &middlewareMetadata, nil
}
