/*
Copyright 2023 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implieout.
See the License for the specific language governing permissions and
limitations under the License.
*/

package wasm

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func BenchmarkExample(b *testing.B) {
	md := metadata.Base{Properties: map[string]string{"url": urlArgsFile}}

	l := logger.NewLogger(b.Name())
	l.SetOutput(io.Discard)

	output := NewWasmOutput(l)
	defer output.(io.Closer).Close()

	ctx := context.Background()
	err := output.Init(ctx, bindings.Metadata{Base: md})
	if err != nil {
		b.Fatal(err)
	}

	request := &bindings.InvokeRequest{Operation: ExecuteOperation}
	// args returns argv back as stdout. Since we aren't doing adding any in
	// the request, the only arg we expect is the program name: "main"
	expected := []byte("main")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if resp, err := output.Invoke(ctx, request); err != nil {
			b.Fatal(err)
		} else if !bytes.Equal(expected, resp.Data) {
			b.Fatal(fmt.Errorf("unexpected response data: %s", string(resp.Data)))
		}
	}
}
