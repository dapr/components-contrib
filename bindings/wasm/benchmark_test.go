package wasm

import (
	"context"
	"io"
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func BenchmarkExample(b *testing.B) {
	path := "./testdata/main.wasm"
	md := metadata.Base{Properties: map[string]string{"path": path}}

	l := logger.NewLogger(b.Name())
	l.SetOutput(io.Discard)

	output := NewWasmOutput(l)
	defer output.(io.Closer).Close()

	ctx := context.Background()
	err := output.Init(ctx, bindings.Metadata{Base: md})
	if err != nil {
		b.Fatal(err)
	}

	request := &bindings.InvokeRequest{
		Metadata:  map[string]string{"args": "salaboy"},
		Operation: bindings.GetOperation,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := output.Invoke(ctx, request); err != nil {
			b.Fatal(err)
		}
	}
}
