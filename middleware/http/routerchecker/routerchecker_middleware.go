package routerchecker

import (
	"encoding/json"
	"regexp"

	"github.com/dapr/components-contrib/middleware"
	"github.com/valyala/fasthttp"
)

// Metadata is the routerchecker middleware config
type routerCheckerMiddlewareMetadata struct {
	Rule string `json:"rule"`
}

// NewRouterCheckerMiddleware returns a new routerchecker middleware
func NewRouterCheckerMiddleware() *Middleware {
	return &Middleware{}
}

// Middleware is an routerchecker middleware
type Middleware struct{}

// GetHandler retruns the HTTP handler provided by the middleware
func (m *Middleware) GetHandler(metadata middleware.Metadata) (
	func(h fasthttp.RequestHandler) fasthttp.RequestHandler, error) {
	meta, err := m.getNativeMetadata(metadata)
	if err != nil {
		return nil, err
	}
	return func(h fasthttp.RequestHandler) fasthttp.RequestHandler {
		return func(ctx *fasthttp.RequestCtx) {
			isMatch, err := m.isMatchRule(meta.Rule, string(ctx.RequestURI()))
			if err != nil {
				ctx.Error("regexp match failed", fasthttp.StatusBadRequest)
				return
			}
			if isMatch {
				ctx.Error("invalid router", fasthttp.StatusBadRequest)
				return
			}
			h(ctx)
		}
	}, nil
}

func (m *Middleware) isMatchRule(rule string, uri string) (b bool, err error) {
	var isMatch bool
	if isMatch, err = regexp.MatchString(rule, uri); err != nil {
		return false, err
	}

	if !isMatch {
		return false, nil
	}
	return true, nil
}

func (m *Middleware) getNativeMetadata(metadata middleware.Metadata) (*routerCheckerMiddlewareMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var middlewareMetadata routerCheckerMiddlewareMetadata
	err = json.Unmarshal(b, &middlewareMetadata)
	if err != nil {
		return nil, err
	}

	return &middlewareMetadata, nil
}
