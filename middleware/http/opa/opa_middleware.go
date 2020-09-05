package opa

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/open-policy-agent/opa/rego"
	"github.com/valyala/fasthttp"
)

type opaMiddlewareMetadata struct {
	Rego            string `json:"rego"`
	IncludedHeaders string `json:"includedHeaders"`
}

// NewOpaMiddleware returns a new Open Policy Agent middleware
func NewOpaMiddleware(logger logger.Logger) *Middleware {
	return &Middleware{logger: logger}
}

// Middleware is an OPA  middleware
type Middleware struct {
	logger logger.Logger
}

// RegoResult is the expected result from rego policy
type RegoResult struct {
	Allow             bool              `json:"allow"`
	AdditionalHeaders map[string]string `json:"additional_headers,omitempty"`
	StatusCode        int               `json:"status_code,omitempty"`
}

// GetHandler retruns the HTTP handler provided by the middleware
func (m *Middleware) GetHandler(metadata middleware.Metadata) (func(h fasthttp.RequestHandler) fasthttp.RequestHandler, error) {
	meta, err := m.getNativeMetadata(metadata)

	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	query, err := rego.New(
		rego.Query("result = data.http.allow"),
		rego.Module("inline.rego", meta.Rego),
	).PrepareForEval(ctx)

	if err != nil {
		return nil, err
	}

	return func(h fasthttp.RequestHandler) fasthttp.RequestHandler {
		return func(ctx *fasthttp.RequestCtx) {
			if handled := m.evalRequest(ctx, meta, &query); handled {
				return
			}
			h(ctx)
		}
	}, nil
}

func (m *Middleware) evalRequest(ctx *fasthttp.RequestCtx, meta *opaMiddlewareMetadata, query *rego.PreparedEvalQuery) bool {
	headers := map[string]string{}
	var allowedHeaders = strings.Split(meta.IncludedHeaders, ",")
	ctx.Request.Header.VisitAll(func(key, value []byte) {
		for _, allowed := range allowedHeaders {
			if string(key) == allowed {
				headers[string(key)] = string(value)
			}
		}
	})

	queryArgs := map[string][]string{}
	ctx.QueryArgs().VisitAll(func(key, value []byte) {
		if val, ok := queryArgs[string(key)]; ok {
			queryArgs[string(key)] = append(val, string(value))
		} else {
			queryArgs[string(key)] = []string{string(value)}
		}
	})

	input := map[string]interface{}{
		"request": map[string]interface{}{
			"method":    string(ctx.Method()),
			"path":      string(ctx.Path()),
			"raw_query": string(ctx.QueryArgs().QueryString()),
			"query":     queryArgs,
			"headers":   headers,
			"scheme":    string(ctx.Request.URI().Scheme()),
			//TODO: allow opting into body support? Reading body isn't efficient
			//TODO: flow parsed token from other http middlewares?
		},
	}

	results, err := query.Eval(context.TODO(), rego.EvalInput(input))

	if err != nil {
		m.opaError(ctx, err)
		return false
	} else if len(results) == 0 {
		m.opaError(ctx, errors.New("received no results back from rego policy. Are you setting data.http.allow?"))
		return false
	} else if allowed := m.handleRegoResult(ctx, results[0].Bindings["result"]); !allowed {
		return false
	}
	return true
}

func (m *Middleware) handleRegoResult(ctx *fasthttp.RequestCtx, result interface{}) bool {
	if allowed, ok := result.(bool); ok {
		if !allowed {
			ctx.Error(fasthttp.StatusMessage(fasthttp.StatusUnauthorized), fasthttp.StatusUnauthorized)
			return false
		}
		return true
	}

	if _, ok := result.(map[string]interface{}); !ok {
		m.opaError(ctx, errors.New("got an invalid type back from repo policy. Only a boolean or map is valid"))
		return false
	}

	// Is it expensive to marshal back and forth? Should we just manually pull out properties?
	marshaled, err := json.Marshal(result)
	if err != nil {
		m.opaError(ctx, err)
		return false
	}

	regoResult := RegoResult{
		StatusCode:        -1,
		AdditionalHeaders: make(map[string]string),
	}

	if err = json.Unmarshal(marshaled, &regoResult); err != nil {
		m.opaError(ctx, err)
		return false
	}

	if !regoResult.Allow {
		if regoResult.StatusCode >= 100 {
			ctx.Response.SetStatusCode(regoResult.StatusCode)
		}
		for key, value := range regoResult.AdditionalHeaders {
			ctx.Response.Header.Set(key, value)
		}
		return false
	}

	for key, value := range regoResult.AdditionalHeaders {
		ctx.Request.Header.Set(key, value)
	}
	return true
}

func (m *Middleware) opaError(ctx *fasthttp.RequestCtx, err error) {
	ctx.Error(fasthttp.StatusMessage(fasthttp.StatusUnauthorized), fasthttp.StatusUnauthorized)
	ctx.Response.Header.Set("x-dapr-opa-error", "1")
	m.logger.Warnf("Error procesing rego policy: %v", err)
}

func (m *Middleware) getNativeMetadata(metadata middleware.Metadata) (*opaMiddlewareMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var middlewareMetadata opaMiddlewareMetadata
	err = json.Unmarshal(b, &middlewareMetadata)
	if err != nil {
		return nil, err
	}
	return &middlewareMetadata, nil
}
