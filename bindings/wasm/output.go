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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/tetratelabs/wazero/sys"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

type initMetadata struct {
	// Path is where to load a `%.wasm` file that implements a command,
	// usually compiled to target WASI.
	Path string `json:"path"`

	// guest is WebAssembly binary implementing the waPC guest, loaded from Path.
	guest []byte
}

type outputBinding struct {
	logger        logger.Logger
	runtimeConfig wazero.RuntimeConfig
	moduleConfig  wazero.ModuleConfig

	runtime wazero.Runtime
	module  wazero.CompiledModule

	instanceCounter uint64
}

var (
	_ bindings.OutputBinding = (*outputBinding)(nil)
	_ io.Closer              = (*outputBinding)(nil)
)

func NewWasmOutput(logger logger.Logger) bindings.OutputBinding {
	return &outputBinding{
		logger: logger,

		// The below ensures context cancels in-flight wasm functions.
		runtimeConfig: wazero.NewRuntimeConfig(),

		// The below violate sand-boxing, but allow code to behave as expecteout.
		moduleConfig: wazero.NewModuleConfig().
			WithRandSource(rand.Reader).
			WithSysNanosleep().
			WithSysWalltime().
			WithSysNanosleep(),
	}
}

func (out *outputBinding) Init(ctx context.Context, metadata bindings.Metadata) (err error) {
	meta, err := out.getInitMetadata(metadata)
	if err != nil {
		return fmt.Errorf("wasm: failed to parse metadata: %w", err)
	}

	out.runtime = wazero.NewRuntimeWithConfig(ctx, out.runtimeConfig)
	out.module, err = out.runtime.CompileModule(ctx, meta.guest)
	if err != nil {
		_ = out.runtime.Close(ctx)
		return fmt.Errorf("wasm: error compiling binary: %w", err)
	}

	switch detectImports(out.module.ImportedFunctions()) {
	case modeWasiP1:
		_, err = wasi_snapshot_preview1.Instantiate(ctx, out.runtime)
	}

	if err != nil {
		return fmt.Errorf("wasm: error instantiating host functions: %w", err)
	}
	return
}

func (out *outputBinding) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	// Currently, concurrent modules can conflict on name. Make sure we have
	// a unique one.
	moduleName := strconv.FormatUint(atomic.AddUint64(&out.instanceCounter, 1), 10)
	moduleConfig := out.moduleConfig.WithName(moduleName)

	if len(req.Data) > 0 {
		moduleConfig = moduleConfig.WithStdin(bytes.NewReader(req.Data))
	}

	var stdout bytes.Buffer
	moduleConfig = moduleConfig.WithStdout(&stdout)

	if args, ok := req.Metadata["args"]; ok {
		argsSlice := strings.Split(args, ",")
		// prepend arg0 (program name) unless we want users to supply it.
		argsSlice = append([]string{out.module.Name()}, argsSlice...)
		moduleConfig = moduleConfig.WithArgs(argsSlice...)
	}

	// Instantiating executes the guest's main function (exported as _start).
	_, err := out.runtime.InstantiateModule(ctx, out.module, moduleConfig)

	// Return stdout if there was no error.
	if err == nil {
		return &bindings.InvokeResponse{Data: stdout.Bytes()}, nil
	}

	// Handle any error, noting exit zero is not one.
	if exitErr, ok := err.(*sys.ExitError); ok && exitErr.ExitCode() == 0 {
		return &bindings.InvokeResponse{Data: stdout.Bytes()}, nil
	}
	return nil, err
}

func (out *outputBinding) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.GetOperation}
}

// Close implements io.Closer
func (out *outputBinding) Close() error {
	// wazero's runtime closes everything.
	if rt := out.runtime; rt != nil {
		return rt.Close(context.Background())
	}
	return nil
}

func (out *outputBinding) getInitMetadata(metadata bindings.Metadata) (*initMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var data initMetadata
	err = json.Unmarshal(b, &data)
	if err != nil {
		return nil, err
	}

	if data.Path == "" {
		return nil, errors.New("missing path")
	}

	data.guest, err = os.ReadFile(data.Path)
	if err != nil {
		return nil, fmt.Errorf("wasm: error reading path: %w", err)
	}

	return &data, nil
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
