/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
	"github.com/dapr/kit/utils"
)

const (
	MTLSRootCA     = "MTLSRootCA"
	MTLSClientCert = "MTLSClientCert"
	MTLSClientKey  = "MTLSClientKey"

	TraceparentHeaderKey            = "traceparent"
	TracestateHeaderKey             = "tracestate"
	TraceMetadataKey                = "traceHeaders"
	securityToken                   = "securityToken"
	securityTokenHeader             = "securityTokenHeader"
	defaultMaxResponseBodySizeBytes = 100 << 20 // 100 MB
)

// HTTPSource is a binding for an http url endpoint invocation
//
//revive:disable-next-line
type HTTPSource struct {
	metadata      httpMetadata
	client        *http.Client
	errorIfNot2XX bool
	logger        logger.Logger
}

type httpMetadata struct {
	URL                 string         `mapstructure:"url"`
	MTLSClientCert      string         `mapstructure:"mtlsClientCert"`
	MTLSClientKey       string         `mapstructure:"mtlsClientKey"`
	MTLSRootCA          string         `mapstructure:"mtlsRootCA"`
	MTLSRenegotiation   string         `mapstructure:"mtlsRenegotiation"`
	SecurityToken       string         `mapstructure:"securityToken"`
	SecurityTokenHeader string         `mapstructure:"securityTokenHeader"`
	ResponseTimeout     *time.Duration `mapstructure:"responseTimeout"`
	// Maximum response to read from HTTP response bodies.
	// This can either be an integer which is interpreted in bytes, or a string with an added unit such as Mi.
	// A value <= 0 means no limit.
	// Default: 100MB
	MaxResponseBodySize kitmd.ByteSize `mapstructure:"maxResponseBodySize"`

	maxResponseBodySizeBytes int64
}

// NewHTTP returns a new HTTPSource.
func NewHTTP(logger logger.Logger) bindings.OutputBinding {
	return &HTTPSource{
		logger: logger,
	}
}

// Init performs metadata parsing.
func (h *HTTPSource) Init(_ context.Context, meta bindings.Metadata) error {
	h.metadata = httpMetadata{
		MaxResponseBodySize: kitmd.NewByteSize(defaultMaxResponseBodySizeBytes),
	}
	err := kitmd.DecodeMetadata(meta.Properties, &h.metadata)
	if err != nil {
		return err
	}

	tlsConfig, err := h.addRootCAToCertPool()
	if err != nil {
		return err
	}
	if tlsConfig == nil {
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if h.metadata.MTLSClientCert != "" && h.metadata.MTLSClientKey != "" {
		err = h.readMTLSClientCertificates(tlsConfig)
		if err != nil {
			return err
		}
	}
	if h.metadata.MTLSRenegotiation != "" {
		err = h.setTLSRenegotiation(tlsConfig)
		if err != nil {
			return err
		}
	}

	h.metadata.maxResponseBodySizeBytes, err = h.metadata.MaxResponseBodySize.GetBytes()
	if err != nil {
		return fmt.Errorf("invalid value for maxResponseBodySize: %w", err)
	}

	// See guidance on proper HTTP client settings here:
	// https://medium.com/@nate510/don-t-use-go-s-default-http-client-4804cb19f779
	dialer := &net.Dialer{
		Timeout: 15 * time.Second,
	}
	netTransport := http.DefaultTransport.(*http.Transport).Clone()
	netTransport.DialContext = dialer.DialContext
	netTransport.TLSHandshakeTimeout = 15 * time.Second
	netTransport.TLSClientConfig = tlsConfig

	h.client = &http.Client{
		Timeout:   0, // no time out here, we use request timeouts instead
		Transport: netTransport,
	}

	if val := meta.Properties["errorIfNot2XX"]; val != "" {
		h.errorIfNot2XX = utils.IsTruthy(val)
	} else {
		// Default behavior
		h.errorIfNot2XX = true
	}

	return nil
}

// readMTLSClientCertificates reads the certificates and key from the metadata and returns a tls.Config.
func (h *HTTPSource) readMTLSClientCertificates(tlsConfig *tls.Config) error {
	clientCertBytes, err := h.getPemBytes(MTLSClientCert, h.metadata.MTLSClientCert)
	if err != nil {
		return err
	}
	clientKeyBytes, err := h.getPemBytes(MTLSClientKey, h.metadata.MTLSClientKey)
	if err != nil {
		return err
	}
	cert, err := tls.X509KeyPair(clientCertBytes, clientKeyBytes)
	if err != nil {
		return fmt.Errorf("failed to load client certificate: %w", err)
	}
	tlsConfig.Certificates = []tls.Certificate{cert}
	return nil
}

// setTLSRenegotiation set TLS renegotiation parameter and returns a tls.Config
func (h *HTTPSource) setTLSRenegotiation(tlsConfig *tls.Config) error {
	switch h.metadata.MTLSRenegotiation {
	case "RenegotiateNever":
		tlsConfig.Renegotiation = tls.RenegotiateNever
	case "RenegotiateOnceAsClient":
		tlsConfig.Renegotiation = tls.RenegotiateOnceAsClient
	case "RenegotiateFreelyAsClient":
		tlsConfig.Renegotiation = tls.RenegotiateFreelyAsClient
	default:
		return fmt.Errorf("invalid renegotiation value: %s", h.metadata.MTLSRenegotiation)
	}
	return nil
}

// Add Root CA cert to the pool of trusted certificates.
// This is required for the client to trust the server certificate in case of HTTPS connection.
func (h *HTTPSource) addRootCAToCertPool() (*tls.Config, error) {
	if h.metadata.MTLSRootCA == "" {
		return nil, nil
	}
	caCertBytes, err := h.getPemBytes(MTLSRootCA, h.metadata.MTLSRootCA)
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCertBytes) {
		return nil, errors.New("failed to add root certificate to certpool")
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    caCertPool,
	}, nil
}

// getPemBytes returns the PEM encoded bytes from the provided certName and certData.
// If the certData is a PEM encoded string, it returns the bytes.
// If there is an error in decoding the PEM, assume it is a filepath and try to read its content.
// Return the error occurred while reading the file.
func (h *HTTPSource) getPemBytes(certName, certData string) ([]byte, error) {
	if !isValidPEM(certData) {
		// Read the file
		pemBytes, err := os.ReadFile(certData)
		if err != nil {
			return nil, fmt.Errorf("provided %q value is neither a valid file path or nor a valid pem encoded string: %w", certName, err)
		}
		return pemBytes, nil
	}
	return []byte(certData), nil
}

// isValidPEM validates the provided input has PEM formatted block.
func isValidPEM(val string) bool {
	block, _ := pem.Decode([]byte(val))
	return block != nil
}

// Operations returns the supported operations for this binding.
func (h *HTTPSource) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.CreateOperation, // For backward compatibility
		"get",
		"head",
		"post",
		"put",
		"patch",
		"delete",
		"options",
		"trace",
	}
}

// Invoke performs an HTTP request to the configured HTTP endpoint.
func (h *HTTPSource) Invoke(parentCtx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	u := h.metadata.URL

	errorIfNot2XX := h.errorIfNot2XX // Default to the component config (default is true)

	if req.Metadata == nil {
		// Prevent things below from failing if req.Metadata is nil.
		req.Metadata = make(map[string]string, 0)
	}

	if req.Metadata["path"] != "" {
		u = strings.TrimRight(u, "/") + "/" + strings.TrimLeft(req.Metadata["path"], "/")
	}
	if req.Metadata["errorIfNot2XX"] != "" {
		errorIfNot2XX = utils.IsTruthy(req.Metadata["errorIfNot2XX"])
	}

	var body io.Reader
	method := strings.ToUpper(string(req.Operation))
	// For backward compatibility
	if method == "CREATE" {
		method = "POST"
	}
	switch method {
	case "PUT", "POST", "PATCH":
		body = bytes.NewBuffer(req.Data)
	case "GET", "HEAD", "DELETE", "OPTIONS", "TRACE":
	default:
		return nil, fmt.Errorf("invalid operation: %s", req.Operation)
	}

	ctx := parentCtx
	if h.metadata.ResponseTimeout != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(parentCtx, *h.metadata.ResponseTimeout)
		defer cancel()
	}

	request, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}

	// Set default values for Content-Type and Accept headers.
	if body != nil {
		if _, ok := req.Metadata["Content-Type"]; !ok {
			request.Header.Set("Content-Type", "application/json; charset=utf-8")
		}
	}
	if _, ok := req.Metadata["Accept"]; !ok {
		request.Header.Set("Accept", "application/json; charset=utf-8")
	}

	// Set security token values if set.
	if h.metadata.SecurityToken != "" && h.metadata.SecurityTokenHeader != "" {
		request.Header.Set(h.metadata.SecurityTokenHeader, h.metadata.SecurityToken)
	}

	// Any metadata keys that start with a capital letter
	// are treated as request headers
	for mdKey, mdValue := range req.Metadata {
		if len(mdKey) > 0 && (mdKey[0] >= 'A' && mdKey[0] <= 'Z') {
			request.Header.Set(mdKey, mdValue)
		}
	}

	// HTTP binding needs to inject traceparent header for proper tracing stack.
	if tp, ok := req.Metadata[TraceparentHeaderKey]; ok && tp != "" {
		if _, ok := request.Header[http.CanonicalHeaderKey(TraceparentHeaderKey)]; ok {
			h.logger.Warn("Tracing is enabled. A custom Traceparent request header cannot be specified and is ignored.")
		}

		request.Header.Set(TraceparentHeaderKey, tp)
	}
	if ts, ok := req.Metadata[TracestateHeaderKey]; ok && ts != "" {
		if _, ok := request.Header[http.CanonicalHeaderKey(TracestateHeaderKey)]; ok {
			h.logger.Warn("Tracing is enabled. A custom Tracestate request header cannot be specified and is ignored.")
		}

		request.Header.Set(TracestateHeaderKey, ts)
	}

	// Send the question
	resp, err := h.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer func() {
		// Drain before closing
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	var respBody io.Reader = resp.Body
	if h.metadata.maxResponseBodySizeBytes > 0 {
		respBody = io.LimitReader(resp.Body, h.metadata.maxResponseBodySizeBytes)
	}

	// Read the response body. For empty responses (e.g. 204 No Content)
	// `b` will be an empty slice.
	b, err := io.ReadAll(respBody)
	if err != nil {
		return nil, err
	}

	metadata := make(map[string]string, len(resp.Header)+2)
	// Include status code & desc
	metadata["statusCode"] = strconv.Itoa(resp.StatusCode)
	metadata["status"] = resp.Status

	// Response headers are mapped from `map[string][]string` to `map[string]string`
	// where headers with multiple values are delimited with ", ".
	for key, values := range resp.Header {
		metadata[key] = strings.Join(values, ", ")
	}

	// Create an error for non-200 status codes unless suppressed.
	if errorIfNot2XX && resp.StatusCode/100 != 2 {
		err = fmt.Errorf("received status code %d", resp.StatusCode)
	}

	return &bindings.InvokeResponse{
		Data:     b,
		Metadata: metadata,
	}, err
}

// GetComponentMetadata returns the metadata of the component.
func (h *HTTPSource) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := httpMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}

func (h *HTTPSource) Close() error {
	return nil
}
