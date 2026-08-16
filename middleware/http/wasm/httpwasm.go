/*
Copyright 2023 The Dapr Authors
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

package wasm

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"reflect"
	"time"

	"github.com/samyfodil/wazy/imports/http_handler"
	wasmnethttp "github.com/samyfodil/wazy/imports/http_handler/nethttp"

	"github.com/dapr/components-contrib/common/wasm"
	mdutils "github.com/dapr/components-contrib/metadata"
	dapr "github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

type middleware struct {
	logger logger.Logger
}

type Metadata struct {
	// GuestConfig is an optional configuration passed to WASM guests.
	// Users can pass an arbitrary string to be parsed by the guest code.
	GuestConfig string `mapstructure:"guestConfig"`
}

func NewMiddleware(logger logger.Logger) dapr.Middleware {
	return &middleware{logger: logger}
}

func (m *middleware) GetHandler(ctx context.Context, metadata dapr.Metadata) (func(next http.Handler) http.Handler, error) {
	rh, err := m.getHandler(ctx, metadata)
	if err != nil {
		return nil, err
	}
	return rh.requestHandler, nil
}

// getHandler is extracted for unit testing.
func (m *middleware) getHandler(ctx context.Context, metadata dapr.Metadata) (*requestHandler, error) {
	// parse common wasm metadata configuration
	meta, err := wasm.GetInitMetadata(ctx, metadata.Base)
	if err != nil {
		return nil, fmt.Errorf("wasm: failed to parse metadata: %w", err)
	}

	// parse wasm middleware specific metadata
	var middlewareMeta Metadata
	err = kitmd.DecodeMetadata(metadata.Base, &middlewareMeta)
	if err != nil {
		return nil, fmt.Errorf("wasm: failed to parse wasm middleware metadata: %w", err)
	}

	var stdout, stderr bytes.Buffer
	mw, err := wasmnethttp.NewMiddleware(ctx, meta.Guest,
		http_handler.WithLogger(m),
		http_handler.WithModuleConfig(wasm.NewModuleConfig(meta).
			WithName(meta.GuestName).
			WithStdout(&stdout).  // reset per request
			WithStderr(&stderr)), // reset per request
		http_handler.WithGuestConfig([]byte(middlewareMeta.GuestConfig)))
	if err != nil {
		return nil, err
	}

	return &requestHandler{mw: mw, logger: m.logger, stdout: &stdout, stderr: &stderr}, nil
}

// IsEnabled implements the same method as documented on http_handler.Logger.
func (m *middleware) IsEnabled(level http_handler.LogLevel) bool {
	var l logger.LogLevel
	switch level {
	case http_handler.LogLevelError:
		l = logger.ErrorLevel
	case http_handler.LogLevelWarn:
		l = logger.WarnLevel
	case http_handler.LogLevelInfo:
		l = logger.InfoLevel
	case http_handler.LogLevelDebug:
		l = logger.DebugLevel
	default: // same as http_handler.LogLevelNone
		return false
	}
	return m.logger.IsOutputLevelEnabled(l)
}

// Log implements the same method as documented on http_handler.Logger.
func (m *middleware) Log(_ context.Context, level http_handler.LogLevel, message string) {
	switch level {
	case http_handler.LogLevelError:
		m.logger.Error(message)
	case http_handler.LogLevelWarn:
		m.logger.Warn(message)
	case http_handler.LogLevelInfo:
		m.logger.Info(message)
	case http_handler.LogLevelDebug:
		m.logger.Debug(message)
	default: // same as http_handler.LogLevelNone
		return
	}
}

type requestHandler struct {
	mw             wasmnethttp.Middleware
	logger         logger.Logger
	stdout, stderr *bytes.Buffer
}

func (rh *requestHandler) requestHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := rh.mw.NewHandler(r.Context(), next)
		defer func() {
			rh.stdout.Reset()
			rh.stderr.Reset()
		}()

		h.ServeHTTP(w, r)

		if stdout := rh.stdout.String(); len(stdout) > 0 {
			rh.logger.Debugf("wasm stdout: %s", stdout)
		}
		if stderr := rh.stderr.String(); len(stderr) > 0 {
			rh.logger.Debugf("wasm stderr: %s", stderr)
		}
	})
}

// Close implements io.Closer
func (rh *requestHandler) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return rh.mw.Close(ctx)
}

func (m *middleware) GetComponentMetadata() (metadataInfo mdutils.MetadataMap) {
	metadataStruct := wasm.InitMetadata{}
	_ = mdutils.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, mdutils.MiddlewareType)
	return
}
