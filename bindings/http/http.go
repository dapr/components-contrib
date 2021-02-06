// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package http

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
)

// HTTPSource is a binding for an http url endpoint invocation
// nolint:golint
type HTTPSource struct {
	metadata httpMetadata
	client   *http.Client

	logger logger.Logger
}

type httpMetadata struct {
	URL string `mapstructure:"url"`
}

// NewHTTP returns a new HTTPSource
func NewHTTP(logger logger.Logger) *HTTPSource {
	return &HTTPSource{logger: logger}
}

// Init performs metadata parsing
func (h *HTTPSource) Init(metadata bindings.Metadata) error {
	if err := mapstructure.Decode(metadata.Properties, &h.metadata); err != nil {
		return err
	}

	// See guidance on proper HTTP client settings here:
	// https://medium.com/@nate510/don-t-use-go-s-default-http-client-4804cb19f779
	dialer := net.Dialer{
		Timeout: 5 * time.Second,
	}
	var netTransport = http.Transport{
		Dial:                (&dialer).Dial,
		TLSHandshakeTimeout: 5 * time.Second,
	}
	h.client = &http.Client{
		Timeout:   time.Second * 10,
		Transport: &netTransport,
	}

	return nil
}

// Operations returns the supported operations for this binding.
func (h *HTTPSource) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.GetOperation}
}

// Invoke performs an HTTP request to the configured HTTP endpoint.
func (h *HTTPSource) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	u := h.metadata.URL
	if req.Metadata != nil {
		if path, ok := req.Metadata["path"]; ok {
			// Simplicity and no "../../.." type exploits.
			u = fmt.Sprintf("%s/%s", strings.TrimRight(u, "/"), strings.TrimLeft(path, "/"))
			if strings.Contains(u, "..") {
				return nil, errors.Errorf("invalid path: %s", path)
			}
		}
	}

	var body io.Reader
	method := strings.ToUpper(string(req.Operation))
	switch method {
	case "PUT", "POST", "PATCH":
		body = bytes.NewBuffer(req.Data)
	}

	// nolint: noctx
	request, err := http.NewRequest(method, u, body)
	if err != nil {
		return nil, err
	}

	if body != nil {
		request.Header.Set("Content-Type", "application/json; charset=utf-8")
	}
	request.Header.Set("Accept", "application/json; charset=utf-8")

	resp, err := h.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return &bindings.InvokeResponse{
		Data: b,
	}, nil
}
