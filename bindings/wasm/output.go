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
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/stealthrocket/wasi-go/imports/wasi_http"
	"github.com/stealthrocket/wasi-go/imports/wasi_http/default_http"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/common/wasm"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// ExecuteOperation is defined here as it isn't in the bindings package.
const ExecuteOperation bindings.OperationKind = "execute"

type outputBinding struct {
	logger        logger.Logger
	runtimeConfig wazero.RuntimeConfig

	meta    *wasm.InitMetadata
	runtime wazero.Runtime
	module  wazero.CompiledModule

	instanceCounter atomic.Uint64
}

var (
	_ bindings.OutputBinding = (*outputBinding)(nil)
	_ io.Closer              = (*outputBinding)(nil)
)

func NewWasmOutput(logger logger.Logger) bindings.OutputBinding {
	return &outputBinding{
		logger: logger,

		// The below ensures context cancels in-flight wasm functions.
		runtimeConfig: wazero.NewRuntimeConfig().
			WithCloseOnContextDone(true),
	}
}

func (out *outputBinding) Init(ctx context.Context, metadata bindings.Metadata) (err error) {
	if out.meta, err = wasm.GetInitMetadata(ctx, metadata.Base); err != nil {
		return fmt.Errorf("wasm: failed to parse metadata: %w", err)
	}

	// Create the runtime, which when closed releases any resources associated with it.
	out.runtime = wazero.NewRuntimeWithConfig(ctx, out.runtimeConfig)

	// Compile the module, which reduces execution time of Invoke
	out.module, err = out.runtime.CompileModule(ctx, out.meta.Guest)
	if err != nil {
		_ = out.runtime.Close(context.Background())
		return fmt.Errorf("wasm: error compiling binary: %w", err)
	}

	imports := detectImports(out.module.ImportedFunctions())

	if _, found := imports[modeWasiP1]; found {
		_, err = wasi_snapshot_preview1.Instantiate(ctx, out.runtime)
	}
	if err != nil {
		_ = out.runtime.Close(context.Background())
		return fmt.Errorf("wasm: error instantiating host wasi functions: %w", err)
	}
	if _, found := imports[modeWasiHTTP]; found {
		if out.meta.StrictSandbox {
			_ = out.runtime.Close(context.Background())
			return errors.New("can not instantiate wasi-http with strict sandbox")
		}
		err = wasi_http.MakeWasiHTTP().Instantiate(ctx, out.runtime)
	}
	if err != nil {
		_ = out.runtime.Close(context.Background())
		return fmt.Errorf("wasm: error instantiating host wasi-http functions: %w", err)
	}
	return nil
}

func (out *outputBinding) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	guestName := out.meta.GuestName
	if guestName == "" {
		guestName = out.module.Name()
	}

	// Currently, concurrent modules can conflict on name. Make sure we have
	// a unique one.
	instanceNum := out.instanceCounter.Add(1)
	instanceName := guestName + "-" + strconv.FormatUint(instanceNum, 10)
	moduleConfig := wasm.NewModuleConfig(out.meta).WithName(instanceName)

	// Only assign STDIN if it is present in the request.
	if len(req.Data) > 0 {
		moduleConfig = moduleConfig.WithStdin(bytes.NewReader(req.Data))
	}

	// Any STDOUT is returned as a result: capture it into a buffer.
	var stdout bytes.Buffer
	moduleConfig = moduleConfig.WithStdout(&stdout)

	// Set the program name to the binary name
	argsSlice := []string{guestName}

	// Get any remaining args from configuration
	if args := req.Metadata["args"]; args != "" {
		argsSlice = append(argsSlice, strings.Split(args, ",")...)
	}
	moduleConfig = moduleConfig.WithArgs(argsSlice...)

	// Instantiating executes the guest's main function (exported as _start).
	mod, err := out.runtime.InstantiateModule(ctx, out.module, moduleConfig)
	if err != nil {
		return nil, err
	}

	// WASI typically calls proc_exit which exits the module, but just in case
	// it doesn't, close the module manually.
	_ = mod.Close(ctx)

	// Return STDOUT if there was no error.
	return &bindings.InvokeResponse{Data: stdout.Bytes()}, nil
}

func (out *outputBinding) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		ExecuteOperation,
	}
}

// Close implements io.Closer
func (out *outputBinding) Close() error {
	// wazero's runtime closes everything.
	if rt := out.runtime; rt != nil {
		return rt.Close(context.Background())
	}
	return nil
}

// In the future
const (
	modeDefault importMode = iota
	modeWasiP1
	modeWasiHTTP
)

type importMode uint

func detectImports(imports []api.FunctionDefinition) map[importMode]bool {
	result := make(map[importMode]bool)
	for _, f := range imports {
		moduleName, _, _ := f.Import()
		switch moduleName {
		case wasi_snapshot_preview1.ModuleName:
			result[modeWasiP1] = true
		case default_http.ModuleName:
			result[modeWasiHTTP] = true
		}
	}
	return result
}

// GetComponentMetadata returns the metadata of the component.
func (out *outputBinding) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := wasm.InitMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}
