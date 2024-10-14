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

package kitex

import (
	"context"
	"sync"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

type kitexOutputBinding struct {
	ctxCache  map[string]*kitexContext
	cacheLock sync.RWMutex
	logger    logger.Logger
}

func NewKitexOutput(logger logger.Logger) bindings.OutputBinding {
	return &kitexOutputBinding{
		ctxCache: make(map[string]*kitexContext),
		logger:   logger,
	}
}

func (out *kitexOutputBinding) Init(_ context.Context, _ bindings.Metadata) error {
	return nil
}

func (out *kitexOutputBinding) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var (
		kitexContextCache *kitexContext
		ok                bool
		finalResult       = &bindings.InvokeResponse{}
	)

	kitexCtx := newKitexContext(req.Metadata)
	kitexCtxKey := kitexCtx.String()

	out.cacheLock.RLock()
	if kitexContextCache, ok = out.ctxCache[kitexCtxKey]; !ok {
		out.cacheLock.RUnlock()
		out.cacheLock.Lock()

		if kitexContextCache, ok = out.ctxCache[kitexCtxKey]; !ok {
			if err := kitexCtx.Init(req.Metadata); err != nil {
				out.cacheLock.Unlock()
				return finalResult, err
			}
			out.ctxCache[kitexCtxKey] = kitexCtx
			kitexContextCache = kitexCtx
		}
		out.cacheLock.Unlock()
	} else {
		out.cacheLock.RUnlock()
	}

	rsp, err := kitexContextCache.Invoke(ctx, req.Data)
	if data, ok := rsp.([]byte); ok {
		finalResult.Data = data
	}

	return finalResult, err
}

func (out *kitexOutputBinding) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.GetOperation}
}

// GetComponentMetadata returns the metadata of the component.
func (out *kitexOutputBinding) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	return
}

func (out *kitexOutputBinding) Close() error {
	return nil
}
