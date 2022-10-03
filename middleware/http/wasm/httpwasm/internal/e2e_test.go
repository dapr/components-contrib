package internal_test

import (
	"fmt"
	"github.com/dapr/components-contrib/middleware/http/wasm/httpwasm/internal/test"
	"log"
	"os"
	"path"
	"strconv"
	"testing"

	"github.com/dapr/components-contrib/metadata"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/components-contrib/middleware/http/wasm/httpwasm"
)

var guestWasm map[string][]byte

const (
	guestWasmOutput = "output"
)

// TestMain ensures we can read the test wasm prior to running e2e tests.
func TestMain(m *testing.M) {
	wasms := []string{guestWasmOutput}
	guestWasm = make(map[string][]byte, len(wasms))
	for _, name := range wasms {
		if wasm, err := os.ReadFile(path.Join("e2e-guests", name, "main.wasm")); err != nil {
			log.Panicln(err)
		} else {
			guestWasm[name] = wasm
		}
	}
	os.Exit(m.Run())
}

func Test_EndToEnd(t *testing.T) {
	type testCase struct {
		name     string
		guest    []byte
		poolSize int
		test     func(t *testing.T, handler fasthttp.RequestHandler, log fmt.Stringer)
	}

	tests := []testCase{
		{
			name:  "consoleLog stdout and stderr",
			guest: guestWasm[guestWasmOutput],
			test: func(t *testing.T, handler fasthttp.RequestHandler, log fmt.Stringer) {
				var ctx fasthttp.RequestCtx
				handler(&ctx)

				// First, we expect any console logging written inline from
				// init (main) and the request (rewrite) funcs to info level.
				//
				// Then, we expect to see stdout and stderr from both scopes
				// at debug level.
				require.Equal(t, `Info(main ConsoleLog)
Info(request[0] ConsoleLog)
Debug(wasm stdout: main Stdout
request[0] Stdout
)
Debug(wasm stderr: main Stderr
request[0] Stderr
)
`, log.String())
			},
		},
	}

	for _, tt := range tests {
		tc := tt
		t.Run(tc.name, func(t *testing.T) {
			poolSize := "1"
			if tc.poolSize > 0 {
				poolSize = strconv.Itoa(tc.poolSize)
			}

			wasmPath := path.Join(t.TempDir(), "guest.wasm")
			require.NoError(t, os.WriteFile(wasmPath, tc.guest, 0o600))

			meta := metadata.Base{Properties: map[string]string{"path": wasmPath, "poolSize": poolSize}}
			l := test.NewLogger()
			handlerFn, err := basic.NewMiddleware(l).GetHandler(middleware.Metadata{Base: meta})
			require.NoError(t, err)
			tc.test(t, handlerFn(func(*fasthttp.RequestCtx) {}), l.(fmt.Stringer))
		})
	}
}
