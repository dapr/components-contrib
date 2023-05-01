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
	"crypto/rand"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/internal/wasm"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// ExecuteOperation is defined here as it isn't in the bindings package.
const ExecuteOperation bindings.OperationKind = "execute"

type outputBinding struct {
	logger        logger.Logger
	runtimeConfig wazero.RuntimeConfig
	moduleConfig  wazero.ModuleConfig

	guestName string
	runtime   wazero.Runtime
	module    wazero.CompiledModule

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

		// The below violate sand-boxing, but allow code to behave as expected.
		moduleConfig: wazero.NewModuleConfig().
			WithRandSource(rand.Reader).
			WithSysWalltime().
			WithSysNanosleep(),
	}
}

func (out *outputBinding) Init(ctx context.Context, metadata bindings.Metadata) (err error) {
	meta, err := wasm.GetInitMetadata(ctx, metadata.Base)
	if err != nil {
		return fmt.Errorf("wasm: failed to parse metadata: %w", err)
	}

	out.guestName = meta.GuestName

	// Create the runtime, which when closed releases any resources associated with it.
	out.runtime = wazero.NewRuntimeWithConfig(ctx, out.runtimeConfig)

	// Compile the module, which reduces execution time of Invoke
	out.module, err = out.runtime.CompileModule(ctx, meta.Guest)
	if err != nil {
		_ = out.runtime.Close(context.Background())
		return fmt.Errorf("wasm: error compiling binary: %w", err)
	}

	switch detectImports(out.module.ImportedFunctions()) {
	case modeWasiP1:
		_, err = wasi_snapshot_preview1.Instantiate(ctx, out.runtime)
	}

	if err != nil {
		_ = out.runtime.Close(context.Background())
		return fmt.Errorf("wasm: error instantiating host functions: %w", err)
	}
	return
}

func (out *outputBinding) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	// Currently, concurrent modules can conflict on name. Make sure we have
	// a unique one.
	instanceNum := out.instanceCounter.Add(1)
	instanceName := out.guestName + "-" + strconv.FormatUint(instanceNum, 10)
	moduleConfig := out.moduleConfig.WithName(instanceName)

	// Only assign STDIN if it is present in the request.
	if len(req.Data) > 0 {
		moduleConfig = moduleConfig.WithStdin(bytes.NewReader(req.Data))
	}

	// Any STDOUT is returned as a result: capture it into a buffer.
	var stdout bytes.Buffer
	moduleConfig = moduleConfig.WithStdout(&stdout)

	// Set the program name to the binary name
	argsSlice := []string{out.guestName}

	// Get any remaining args from configuration
	if args := req.Metadata["args"]; args != "" {
		argsSlice = append(argsSlice, strings.Split(args, ",")...)
	}
	moduleConfig = moduleConfig.WithArgs(argsSlice...)

	// Instantiating executes the guest's main function (exported as _start).
	mod, err := out.runtime.InstantiateModule(ctx, out.module, moduleConfig)

	// WASI typically calls proc_exit which exits the module, but just in case
	// it doesn't, close the module manually.
	_ = mod.Close(ctx)

	// Return STDOUT if there was no error.
	if err == nil {
		return &bindings.InvokeResponse{Data: stdout.Bytes()}, nil
	}
	return nil, err
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
)

type importMode uint

func detectImports(imports []api.FunctionDefinition) importMode {
	for _, f := range imports {
		moduleName, _, _ := f.Import()
		switch moduleName {
		case wasi_snapshot_preview1.ModuleName:
			return modeWasiP1
		}
	}
	return modeDefault
}

// GetComponentMetadata returns the metadata of the component.
func (out *outputBinding) GetComponentMetadata() map[string]string {
	metadataStruct := wasm.InitMetadata{}
	metadataInfo := map[string]string{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return metadataInfo
}
