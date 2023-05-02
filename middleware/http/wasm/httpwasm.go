package wasm

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"reflect"
	"time"

	"github.com/http-wasm/http-wasm-host-go/api"
	"github.com/http-wasm/http-wasm-host-go/handler"
	wasmnethttp "github.com/http-wasm/http-wasm-host-go/handler/nethttp"
	"github.com/tetratelabs/wazero"

	"github.com/dapr/components-contrib/internal/wasm"
	mdutils "github.com/dapr/components-contrib/metadata"
	dapr "github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

type middleware struct {
	logger logger.Logger
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
	meta, err := wasm.GetInitMetadata(ctx, metadata.Base)
	if err != nil {
		return nil, fmt.Errorf("wasm: failed to parse metadata: %w", err)
	}

	var stdout, stderr bytes.Buffer
	mw, err := wasmnethttp.NewMiddleware(ctx, meta.Guest,
		handler.Logger(m),
		handler.ModuleConfig(wazero.NewModuleConfig().
			WithName(meta.GuestName).
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

func (m *middleware) GetComponentMetadata() map[string]string {
	metadataStruct := wasm.InitMetadata{}
	metadataInfo := map[string]string{}
	mdutils.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, mdutils.MiddlewareType)
	return metadataInfo
}
