package wasm

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/http-wasm/http-wasm-host-go/handler"

	wasmnethttp "github.com/http-wasm/http-wasm-host-go/handler/nethttp"

	"github.com/http-wasm/http-wasm-host-go/api"
	"github.com/tetratelabs/wazero"

	dapr "github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

// ctx substitutes for context propagation until middleware APIs support it.
var ctx = context.Background()

// middlewareMetadata includes configuration used for the WebAssembly handler.
// Detailed notes are in README.md for visibility.
//
// Note: When changing this, you must update the docs with summary comments per
// field.
// https://github.com/dapr/docs/blob/v1.9/daprdocs/content/en/reference/components-reference/supported-middleware/middleware-wasm.md
type middlewareMetadata struct {
	// Path is where to load a `%.wasm` file that implements the guest side of
	// the handler protocol. No default.
	Path string `json:"path"`

	// guest is WebAssembly binary implementing the waPC guest, loaded from Path.
	guest []byte
}

type middleware struct {
	logger logger.Logger
}

func NewMiddleware(logger logger.Logger) dapr.Middleware {
	return &middleware{logger: logger}
}

func (m *middleware) GetHandler(metadata dapr.Metadata) (func(next http.Handler) http.Handler, error) {
	rh, err := m.getHandler(metadata)
	if err != nil {
		return nil, err
	}
	return rh.requestHandler, nil
}

// getHandler is extracted for unit testing.
func (m *middleware) getHandler(metadata dapr.Metadata) (*requestHandler, error) {
	meta, err := m.getMetadata(metadata)
	if err != nil {
		return nil, fmt.Errorf("wasm basic: failed to parse metadata: %w", err)
	}

	var stdout, stderr bytes.Buffer
	mw, err := wasmnethttp.NewMiddleware(ctx, meta.guest,
		handler.Logger(m),
		handler.ModuleConfig(wazero.NewModuleConfig().
			WithStdout(&stdout). // reset per request
			WithStderr(&stderr). // reset per request
			// The below violate sand-boxing, but allow code to behave as expected.
			WithRandSource(rand.Reader).
			WithSysNanosleep().
			WithSysWalltime().
			WithSysNanosleep()))
	if err != nil {
		return nil, err
	}

	return &requestHandler{mw: mw, logger: m.logger, stdout: &stdout, stderr: &stderr}, nil
}

// IsEnabled implements the same method as documented on api.Logger.
func (m *middleware) IsEnabled(level api.LogLevel) bool {
	var l logger.LogLevel
	switch level {
	case api.LogLevelError:
		l = logger.ErrorLevel
	case api.LogLevelWarn:
		l = logger.WarnLevel
	case api.LogLevelInfo:
		l = logger.InfoLevel
	case api.LogLevelDebug:
		l = logger.DebugLevel
	default: // same as api.LogLevelNone
		return false
	}
	return m.logger.IsOutputLevelEnabled(l)
}

// Log implements the same method as documented on api.Logger.
func (m *middleware) Log(_ context.Context, level api.LogLevel, message string) {
	switch level {
	case api.LogLevelError:
		m.logger.Error(message)
	case api.LogLevelWarn:
		m.logger.Warn(message)
	case api.LogLevelInfo:
		m.logger.Info(message)
	case api.LogLevelDebug:
		m.logger.Debug(message)
	default: // same as api.LogLevelNone
		return
	}
}

func (m *middleware) getMetadata(metadata dapr.Metadata) (*middlewareMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var data middlewareMetadata
	err = json.Unmarshal(b, &data)
	if err != nil {
		return nil, err
	}

	if data.Path == "" {
		return nil, errors.New("missing path")
	}

	data.guest, err = os.ReadFile(data.Path)
	if err != nil {
		return nil, fmt.Errorf("error reading path: %w", err)
	}

	return &data, nil
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
	return rh.mw.Close(ctx)
}
