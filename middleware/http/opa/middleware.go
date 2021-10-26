package opa

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/open-policy-agent/opa/rego"
	"github.com/valyala/fasthttp"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

type Status int

type middlewareMetadata struct {
	Rego            string `json:"rego"`
	DefaultStatus   Status `json:"defaultStatus,omitempty"`
	IncludedHeaders string `json:"includedHeaders,omitempty"`
}

// NewMiddleware returns a new Open Policy Agent middleware.
func NewMiddleware(logger logger.Logger) *Middleware {
	return &Middleware{logger: logger}
}

// Middleware is an OPA  middleware.
type Middleware struct {
	logger logger.Logger
}

// RegoResult is the expected result from rego policy.
type RegoResult struct {
	Allow             bool              `json:"allow"`
	AdditionalHeaders map[string]string `json:"additional_headers,omitempty"`
	StatusCode        int               `json:"status_code,omitempty"`
}

const opaErrorHeaderKey = "x-dapr-opa-error"

var (
	errOpaNoResult          = errors.New("received no results back from rego policy. Are you setting data.http.allow?")
	errOpaInvalidResultType = errors.New("got an invalid type back from repo policy. Only a boolean or map is valid")
)

func (s *Status) UnmarshalJSON(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		if value != math.Trunc(value) {
			return fmt.Errorf("invalid float value %f parse to status(int)", value)
		}
		*s = Status(value)
	case string:
		intVal, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		*s = Status(intVal)
	default:
		return fmt.Errorf("invalid value %v parse to status(int)", value)
	}
	if !s.Valid() {
		return fmt.Errorf("invalid status value %d expected in range [100-599]", *s)
	}

	return nil
}

// Check status is in the correct range for RFC 2616 status codes [100-599].
func (s *Status) Valid() bool {
	return s != nil && *s >= 100 && *s < 600
}

// GetHandler returns the HTTP handler provided by the middleware.
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

func (m *Middleware) evalRequest(ctx *fasthttp.RequestCtx, meta *middlewareMetadata, query *rego.PreparedEvalQuery) bool {
	headers := map[string]string{}
	allowedHeaders := strings.Split(meta.IncludedHeaders, ",")
	ctx.Request.Header.VisitAll(func(key, value []byte) {
		for _, allowedHeader := range allowedHeaders {
			buf := []byte("")
			result := fasthttp.AppendNormalizedHeaderKeyBytes(buf, []byte(allowedHeader))
			normalizedHeader := result[0:]
			if bytes.Equal(key, normalizedHeader) {
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

	path := string(ctx.Path())
	pathParts := strings.Split(strings.Trim(path, "/"), "/")
	input := map[string]interface{}{
		"request": map[string]interface{}{
			"method":     string(ctx.Method()),
			"path":       path,
			"path_parts": pathParts,
			"raw_query":  string(ctx.QueryArgs().QueryString()),
			"query":      queryArgs,
			"headers":    headers,
			"scheme":     string(ctx.Request.URI().Scheme()),
			"body":       string(ctx.Request.Body()),
		},
	}

	results, err := query.Eval(context.TODO(), rego.EvalInput(input))
	if err != nil {
		m.opaError(ctx, meta, err)

		return false
	}

	if len(results) == 0 {
		m.opaError(ctx, meta, errOpaNoResult)

		return false
	}

	return m.handleRegoResult(ctx, meta, results[0].Bindings["result"])
}

// handleRegoResult takes the in process request and open policy agent evaluation result
// and maps it the appropriate response or headers.
// It returns true if the request should continue, or false if a response should be immediately returned.
func (m *Middleware) handleRegoResult(ctx *fasthttp.RequestCtx, meta *middlewareMetadata, result interface{}) bool {
	if allowed, ok := result.(bool); ok {
		if !allowed {
			ctx.Error(fasthttp.StatusMessage(int(meta.DefaultStatus)), int(meta.DefaultStatus))
		}

		return allowed
	}

	if _, ok := result.(map[string]interface{}); !ok {
		m.opaError(ctx, meta, errOpaInvalidResultType)

		return false
	}

	// Is it expensive to marshal back and forth? Should we just manually pull out properties?
	marshaled, err := json.Marshal(result)
	if err != nil {
		m.opaError(ctx, meta, err)

		return false
	}

	regoResult := RegoResult{
		// By default, a non-allowed request with return a 403 response.
		StatusCode:        int(meta.DefaultStatus),
		AdditionalHeaders: make(map[string]string),
	}

	if err = json.Unmarshal(marshaled, &regoResult); err != nil {
		m.opaError(ctx, meta, err)

		return false
	}

	// If the result isn't allowed, set the response status and
	// apply the additional headers to the response.
	// Otherwise, set the headers on the ongoing request (overriding as necessary).
	if !regoResult.Allow {
		ctx.Error(fasthttp.StatusMessage(regoResult.StatusCode), regoResult.StatusCode)
		for key, value := range regoResult.AdditionalHeaders {
			ctx.Response.Header.Set(key, value)
		}
	} else {
		for key, value := range regoResult.AdditionalHeaders {
			ctx.Request.Header.Set(key, value)
		}
	}

	return regoResult.Allow
}

func (m *Middleware) opaError(ctx *fasthttp.RequestCtx, meta *middlewareMetadata, err error) {
	ctx.Error(fasthttp.StatusMessage(int(meta.DefaultStatus)), int(meta.DefaultStatus))
	ctx.Response.Header.Set(opaErrorHeaderKey, "true")
	m.logger.Warnf("Error procesing rego policy: %v", err)
}

func (m *Middleware) getNativeMetadata(metadata middleware.Metadata) (*middlewareMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	meta := middlewareMetadata{
		DefaultStatus: 403,
	}
	err = json.Unmarshal(b, &meta)
	if err != nil {
		return nil, err
	}

	return &meta, nil
}
