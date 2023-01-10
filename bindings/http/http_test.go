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

package http_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	bindingHttp "github.com/dapr/components-contrib/bindings/http"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func TestOperations(t *testing.T) {
	opers := (*bindingHttp.HTTPSource)(nil).Operations()
	assert.Equal(t, []bindings.OperationKind{
		bindings.CreateOperation,
		"get",
		"head",
		"post",
		"put",
		"patch",
		"delete",
		"options",
		"trace",
	}, opers)
}

type TestCase struct {
	input      string
	operation  string
	metadata   map[string]string
	path       string
	err        string
	statusCode int
}

func (tc TestCase) ToInvokeRequest() bindings.InvokeRequest {
	requestMetadata := tc.metadata

	if requestMetadata == nil {
		requestMetadata = map[string]string{}
	}

	requestMetadata["X-Status-Code"] = strconv.Itoa(tc.statusCode)

	return bindings.InvokeRequest{
		Data:      []byte(tc.input),
		Metadata:  requestMetadata,
		Operation: bindings.OperationKind(tc.operation),
	}
}

type HTTPHandler struct {
	Path string
}

func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	h.Path = req.URL.Path

	input := req.Method
	if req.Body != nil {
		defer req.Body.Close()
		b, _ := io.ReadAll(req.Body)
		if len(b) > 0 {
			input = string(b)
		}
	}
	inputFromHeader := req.Header.Get("X-Input")
	if inputFromHeader != "" {
		input = inputFromHeader
	}

	w.Header().Set("Content-Type", "text/plain")

	statusCode := req.Header.Get("X-Status-Code")
	if statusCode != "" {
		code, _ := strconv.Atoi(statusCode)
		w.WriteHeader(code)
	}

	w.Write([]byte(strings.ToUpper(input)))
}

func NewHTTPHandler() *HTTPHandler {
	return &HTTPHandler{
		Path: "/",
	}
}

func InitBinding(s *httptest.Server, extraProps map[string]string) (bindings.OutputBinding, error) {
	m := bindings.Metadata{Base: metadata.Base{
		Properties: map[string]string{
			"url": s.URL,
		},
	}}

	if extraProps != nil {
		for k, v := range extraProps {
			m.Properties[k] = v
		}
	}

	hs := bindingHttp.NewHTTP(logger.NewLogger("test"))
	err := hs.Init(m)
	return hs, err
}

func TestInit(t *testing.T) {
	handler := NewHTTPHandler()
	s := httptest.NewServer(handler)
	defer s.Close()

	_, err := InitBinding(s, nil)
	require.NoError(t, err)
}

func TestDefaultBehavior(t *testing.T) {
	handler := NewHTTPHandler()
	s := httptest.NewServer(handler)
	defer s.Close()

	hs, err := InitBinding(s, nil)
	require.NoError(t, err)
	verifyDefaultBehaviors(t, hs, handler)
}

func TestNon2XXErrorsSuppressed(t *testing.T) {
	handler := NewHTTPHandler()
	s := httptest.NewServer(handler)
	defer s.Close()

	hs, err := InitBinding(s, map[string]string{"errorIfNot2XX": "false"})
	require.NoError(t, err)
	verifyNon2XXErrorsSuppressed(t, hs, handler)
}

func InitBindingForHTTPS(s *httptest.Server, extraProps map[string]string) (bindings.OutputBinding, error) {
	m := bindings.Metadata{Base: metadata.Base{
		Properties: map[string]string{
			"url": s.URL,
		},
	}}
	for k, v := range extraProps {
		m.Properties[k] = v
	}
	hs := bindingHttp.NewHTTP(logger.NewLogger("test"))
	err := hs.Init(m)
	return hs, err
}

func httpsHandler(w http.ResponseWriter, r *http.Request) {
	// r.TLS gets ignored by HTTP handlers.
	// in case where client auth is not required, r.TLS.PeerCertificates will be empty.
	res := fmt.Sprintf("%v", len(r.TLS.PeerCertificates))
	io.WriteString(w, res)
}

func TestDefaultBehaviorHTTPS(t *testing.T) {
	handler := NewHTTPHandler()
	server := setupHTTPSServer(t, true, handler)
	defer server.Close()

	certMap := map[string]string{
		"MTLSRootCA":     filepath.Join(".", "testdata", "ca.pem"),
		"MTLSClientCert": filepath.Join(".", "testdata", "client.pem"),
		"MTLSClientKey":  filepath.Join(".", "testdata", "client.key"),
	}
	hs, err := InitBindingForHTTPS(server, certMap)
	require.NoError(t, err)

	verifyDefaultBehaviors(t, hs, handler)
}

func TestNon2XXErrorsSuppressedHTTPS(t *testing.T) {
	handler := NewHTTPHandler()
	server := setupHTTPSServer(t, true, handler)
	defer server.Close()

	certMap := map[string]string{
		"MTLSRootCA":     filepath.Join(".", "testdata", "ca.pem"),
		"MTLSClientCert": filepath.Join(".", "testdata", "client.pem"),
		"MTLSClientKey":  filepath.Join(".", "testdata", "client.key"),
		"errorIfNot2XX":  "false",
	}
	hs, err := InitBindingForHTTPS(server, certMap)
	require.NoError(t, err)
	verifyNon2XXErrorsSuppressed(t, hs, handler)
}

func TestHTTPSBinding(t *testing.T) {
	handler := http.NewServeMux()
	handler.HandleFunc("/testhttps", httpsHandler)
	server := setupHTTPSServer(t, true, handler)
	defer server.Close()
	t.Run("get with https with valid client cert and clientAuthEnabled true", func(t *testing.T) {
		certMap := map[string]string{
			"MTLSRootCA":     filepath.Join(".", "testdata", "ca.pem"),
			"MTLSClientCert": filepath.Join(".", "testdata", "client.pem"),
			"MTLSClientKey":  filepath.Join(".", "testdata", "client.key"),
		}
		hs, err := InitBindingForHTTPS(server, certMap)
		require.NoError(t, err)

		req := TestCase{
			input:      "GET",
			operation:  "get",
			metadata:   map[string]string{"path": "/testhttps"},
			path:       "/testhttps",
			err:        "",
			statusCode: 200,
		}.ToInvokeRequest()
		response, err := hs.Invoke(context.Background(), &req)
		assert.NoError(t, err)
		peerCerts, err := strconv.Atoi(string(response.Data))
		assert.NoError(t, err)
		assert.True(t, peerCerts > 0)

		req = TestCase{
			input:      "EXPECTED",
			operation:  "post",
			metadata:   map[string]string{"path": "/testhttps"},
			path:       "/testhttps",
			err:        "",
			statusCode: 201,
		}.ToInvokeRequest()
		response, err = hs.Invoke(context.Background(), &req)
		assert.NoError(t, err)
		peerCerts, err = strconv.Atoi(string(response.Data))
		assert.NoError(t, err)
		assert.True(t, peerCerts > 0)
	})
	t.Run("get with https with no client cert and clientAuthEnabled true", func(t *testing.T) {
		certMap := map[string]string{}
		hs, err := InitBindingForHTTPS(server, certMap)
		require.NoError(t, err)

		req := TestCase{
			input:      "GET",
			operation:  "get",
			metadata:   map[string]string{"path": "/testhttps"},
			path:       "/testhttps",
			err:        "",
			statusCode: 200,
		}.ToInvokeRequest()
		_, err = hs.Invoke(context.Background(), &req)
		assert.Error(t, err)
	})
}

func setupHTTPSServer(t *testing.T, clientAuthEnabled bool, handler http.Handler) *httptest.Server {
	server := httptest.NewUnstartedServer(handler)
	caCertFile, err := os.ReadFile(filepath.Join(".", "testdata", "ca.pem"))
	assert.NoError(t, err)

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCertFile)

	serverCert := filepath.Join(".", "testdata", "server.pem")
	serverKey := filepath.Join(".", "testdata", "server.key")
	cert, err := tls.LoadX509KeyPair(serverCert, serverKey)
	assert.NoError(t, err)

	// Create the TLS Config with the CA pool and enable Client certificate validation
	tlsConfig := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		ClientCAs:    caCertPool,
		Certificates: []tls.Certificate{cert},
	}
	if clientAuthEnabled {
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}
	server.TLS = tlsConfig
	server.StartTLS()
	return server
}

func verifyDefaultBehaviors(t *testing.T, hs bindings.OutputBinding, handler *HTTPHandler) {
	tests := map[string]TestCase{
		"get": {
			input:      "GET",
			operation:  "get",
			metadata:   nil,
			path:       "/",
			err:        "",
			statusCode: 200,
		},
		"request headers": {
			input:      "OVERRIDE",
			operation:  "get",
			metadata:   map[string]string{"X-Input": "override"},
			path:       "/",
			err:        "",
			statusCode: 200,
		},
		"post": {
			input:      "expected",
			operation:  "post",
			metadata:   map[string]string{"path": "/test"},
			path:       "/test",
			err:        "",
			statusCode: 201,
		},
		"put": {
			input:      "expected",
			operation:  "put",
			statusCode: 204,
			metadata:   map[string]string{"path": "/test"},
			path:       "/test",
			err:        "",
		},
		"patch": {
			input:      "expected",
			operation:  "patch",
			metadata:   map[string]string{"path": "/test"},
			path:       "/test",
			err:        "",
			statusCode: 206,
		},
		"delete": {
			input:      "DELETE",
			operation:  "delete",
			metadata:   nil,
			path:       "/",
			err:        "",
			statusCode: 200,
		},
		"options": {
			input:      "OPTIONS",
			operation:  "options",
			metadata:   nil,
			path:       "/",
			err:        "",
			statusCode: 200,
		},
		"trace": {
			input:      "TRACE",
			operation:  "trace",
			metadata:   nil,
			path:       "/",
			err:        "",
			statusCode: 200,
		},
		"backward compatibility": {
			input:      "expected",
			operation:  "create",
			metadata:   map[string]string{"path": "/test"},
			path:       "/test",
			err:        "",
			statusCode: 200,
		},
		"invalid path": {
			input:      "expected",
			operation:  "POST",
			metadata:   map[string]string{"path": "/../test"},
			path:       "",
			err:        "invalid path: /../test",
			statusCode: 400,
		},
		"invalid operation": {
			input:      "notvalid",
			operation:  "notvalid",
			metadata:   map[string]string{"path": "/test"},
			path:       "/test",
			err:        "invalid operation: notvalid",
			statusCode: 400,
		},
		"internal server error": {
			input:      "internal server error",
			operation:  "post",
			metadata:   map[string]string{"path": "/"},
			path:       "/",
			err:        "received status code 500",
			statusCode: 500,
		},
		"internal server error suppressed": {
			input:      "internal server error", // trigger 500 downstream
			operation:  "post",
			metadata:   map[string]string{"path": "/", "errorIfNot2XX": "false"},
			path:       "/",
			err:        "",
			statusCode: 500,
		},
		"redirect should not yield an error": {
			input:      "show me the treasure!",
			operation:  "post",
			metadata:   map[string]string{"path": "/", "errorIfNot2XX": "false"},
			path:       "/",
			err:        "",
			statusCode: 302,
		},
		"redirect results in an error if not suppressed": {
			input:      "show me the treasure!",
			operation:  "post",
			metadata:   map[string]string{"path": "/"},
			path:       "/",
			err:        "received status code 302",
			statusCode: 302,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			req := tc.ToInvokeRequest()
			response, err := hs.Invoke(context.Background(), &req)
			if tc.err == "" {
				require.NoError(t, err)
				assert.Equal(t, tc.path, handler.Path)
				if tc.statusCode != 204 {
					// 204 will return no content, so we should skip checking
					assert.Equal(t, strings.ToUpper(tc.input), string(response.Data))
				}
				assert.Equal(t, "text/plain", response.Metadata["Content-Type"])
			} else {
				require.Error(t, err)
				assert.Equal(t, tc.err, err.Error())
			}
		})
	}
}

func verifyNon2XXErrorsSuppressed(t *testing.T, hs bindings.OutputBinding, handler *HTTPHandler) {
	tests := map[string]TestCase{
		"internal server error": {
			input:      "internal server error",
			operation:  "post",
			metadata:   map[string]string{"path": "/"},
			path:       "/",
			err:        "",
			statusCode: 500,
		},
		"internal server error overridden": {
			input:      "internal server error",
			operation:  "post",
			metadata:   map[string]string{"path": "/", "errorIfNot2XX": "true"},
			path:       "/",
			err:        "received status code 500",
			statusCode: 500,
		},
		"internal server error suppressed by request and component": {
			input:      "internal server error", // trigger 500
			operation:  "post",
			metadata:   map[string]string{"path": "/", "errorIfNot2XX": "false"},
			path:       "/",
			err:        "",
			statusCode: 500,
		},
		"trace": {
			input:      "TRACE",
			operation:  "trace",
			metadata:   nil,
			path:       "/",
			err:        "",
			statusCode: 200,
		},
		"backward compatibility": {
			input:      "expected",
			operation:  "create",
			metadata:   map[string]string{"path": "/test"},
			path:       "/test",
			err:        "",
			statusCode: 200,
		},
		"invalid path": {
			input:      "expected",
			operation:  "POST",
			metadata:   map[string]string{"path": "/../test"},
			path:       "",
			err:        "invalid path: /../test",
			statusCode: 400,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			req := tc.ToInvokeRequest()
			response, err := hs.Invoke(context.Background(), &req)
			if tc.err == "" {
				require.NoError(t, err)
				assert.Equal(t, tc.path, handler.Path)
				if tc.statusCode != 204 {
					// 204 will return no content, so we should skip checking
					assert.Equal(t, strings.ToUpper(tc.input), string(response.Data))
				}
				assert.Equal(t, "text/plain", response.Metadata["Content-Type"])
			} else {
				require.Error(t, err)
				assert.Equal(t, tc.err, err.Error())
			}
		})
	}
}
