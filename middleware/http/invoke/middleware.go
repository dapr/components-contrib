// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// -----------------------------------------------------------

package invoke

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/dapr/components-contrib/middleware"
	"github.com/valyala/fasthttp"
)

// global client
var client *http.Client

type invokeMiddlewareMetadata struct {
	// Invoke URL, such as: http://127.0.0.1:3500/v1.0/invoke/xxx/method/xxx
	InvokeURL string `json:"invokeURL"`
	// Invoke Service Verb, default is POST
	InvokeVerb string `json:"invokeVerb"`
	// Enforce Request Verbs
	EnforceRequestVerbs string `json:"enforceRequestVerbs"`
	// Request Timeout, default is 7 seconds
	Timeout float32 `json:"timeout,string"`
	// Http Client Transport TLS Config
	InsecureSkipVerify bool `json:"insecureSkipVerify,string"`
	// Max Retry Count
	MaxRetry uint16 `json:"maxRetry,string"`
	// Expected Status Code
	ExpectedStatusCode uint16 `json:"expectedStatusCode,string"`
	// Forward URL Header Name, default: x-forward-url
	ForwardURLHeaderName string `json:"forwardURLHeaderName"`
	// Forward Method Header Name, default: x-forward-method
	ForwardMethodHeaderName string `json:"forwardMethodHeaderName"`
}

type Middleware struct {
}

func (m *Middleware) getNativeMetadata(metadata middleware.Metadata) (*invokeMiddlewareMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var middlewareMetadata invokeMiddlewareMetadata
	err = json.Unmarshal(b, &middlewareMetadata)
	if err != nil {
		return nil, err
	}

	return &middlewareMetadata, nil
}

// Get Http client
func (m *Middleware) getClient(metadata *invokeMiddlewareMetadata) *http.Client {
	if client == nil {
		client = &http.Client{
			Timeout: time.Second * time.Duration(metadata.Timeout),
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: metadata.InsecureSkipVerify,
				},
			},
		}
	}

	return client
}

// Forward request to dapr service method
func (m *Middleware) forwardRequest(ctx *fasthttp.RequestCtx, metadata *invokeMiddlewareMetadata) (*http.Response, error) {
	c := m.getClient(metadata)

	// Construct http request
	req, err := http.NewRequest(strings.ToUpper(metadata.InvokeVerb), metadata.InvokeURL, bytes.NewBuffer(ctx.Request.Body()))
	if err != nil {
		return nil, err
	}

	// Set forward info
	req.Header.Set(metadata.ForwardURLHeaderName, ctx.URI().String())
	req.Header.Set(metadata.ForwardMethodHeaderName, string(ctx.Method()))

	// Clone request headers
	ctx.Request.Header.VisitAll(func(key, value []byte) {
		req.Header.Set(string(key), string(value))
	})

	restRetry := metadata.MaxRetry
	for restRetry > 0 {
		resp, err := c.Do(req)

		// enter retry
		if err != nil {
			restRetry--

			continue
		}

		// judge response
		return resp, nil
	}

	return nil, fmt.Errorf("request error count has exceeded max retry count: %d", metadata.MaxRetry)
}

func (m *Middleware) GetHandler(metadata middleware.Metadata) (func(h fasthttp.RequestHandler) fasthttp.RequestHandler, error) {
	// Get invoke middleware metadata
	meta, err := m.getNativeMetadata(metadata)
	if err != nil {
		return nil, err
	}

	// Invoke dapr appid APP method
	return func(h fasthttp.RequestHandler) fasthttp.RequestHandler {
		return func(ctx *fasthttp.RequestCtx) {
			// Judge if request method is enforced
			isEnforced := false
			for _, verb := range strings.Split(meta.EnforceRequestVerbs, ",") {
				if strings.EqualFold(verb, string(ctx.Method())) {
					isEnforced = true

					break
				}
			}
			if !isEnforced {
				h(ctx)

				return
			}

			// Forward request
			resp, err := m.forwardRequest(ctx, meta)
			if err != nil {
				ctx.Error(err.Error(), 400)

				return
			}

			// Judge response status is expected
			if resp.StatusCode == int(meta.ExpectedStatusCode) {
				h(ctx)

				return
			}

			// if not ok
			ctx.Error("deny", 403)
		}
	}, nil
}
