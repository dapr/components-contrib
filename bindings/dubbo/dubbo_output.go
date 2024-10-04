/*
Copyright 2022 The Dapr Authors
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

package dubbo

import (
	"context"
	"sync"

	_ "dubbo.apache.org/dubbo-go/v3/cluster/cluster/failover"
	_ "dubbo.apache.org/dubbo-go/v3/cluster/loadbalance/random"
	"dubbo.apache.org/dubbo-go/v3/common/constant"
	dubboLogger "dubbo.apache.org/dubbo-go/v3/common/logger"
	_ "dubbo.apache.org/dubbo-go/v3/common/proxy/proxy_factory"
	_ "dubbo.apache.org/dubbo-go/v3/filter/filter_impl"
	_ "dubbo.apache.org/dubbo-go/v3/protocol/dubbo"
	dubboImpl "dubbo.apache.org/dubbo-go/v3/protocol/dubbo/impl"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

type DubboOutputBinding struct {
	ctxCache  map[string]*dubboContext
	cacheLock sync.RWMutex
}

var dubboBinding *DubboOutputBinding

func NewDubboOutput(logger logger.Logger) bindings.OutputBinding {
	if dubboBinding == nil {
		dubboBinding = &DubboOutputBinding{
			ctxCache: make(map[string]*dubboContext),
		}
	}
	dubboLogger.SetLogger(logger)
	return dubboBinding
}

func (out *DubboOutputBinding) Init(_ context.Context, _ bindings.Metadata) error {
	dubboImpl.SetSerializer(constant.Hessian2Serialization, HessianSerializer{})
	return nil
}

func (out *DubboOutputBinding) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var (
		cachedDubboCtx *dubboContext
		ok             bool
		finalResult    = &bindings.InvokeResponse{}
	)

	dubboCtx := newDubboContext(req.Metadata)
	dubboCtxKey := dubboCtx.String()

	out.cacheLock.RLock()
	if cachedDubboCtx, ok = out.ctxCache[dubboCtxKey]; !ok {
		out.cacheLock.RUnlock()
		out.cacheLock.Lock()
		// double check
		if cachedDubboCtx, ok = out.ctxCache[dubboCtxKey]; !ok {
			// not found cached dubbo context, init ctx and store into cache
			if err := dubboCtx.Init(); err != nil {
				out.cacheLock.Unlock()
				return finalResult, err
			}
			out.ctxCache[dubboCtxKey] = dubboCtx
			cachedDubboCtx = dubboCtx
		}
		out.cacheLock.Unlock()
	} else {
		out.cacheLock.RUnlock()
	}

	rsp, err := cachedDubboCtx.Invoke(ctx, req.Data)
	if data, ok := rsp.([]byte); ok {
		finalResult.Data = data
	}
	return finalResult, err
}

func (out *DubboOutputBinding) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.GetOperation}
}

// GetComponentMetadata returns the metadata of the component.
func (out *DubboOutputBinding) GetComponentMetadata() metadata.MetadataMap {
	return metadata.MetadataMap{}
}

func (out *DubboOutputBinding) Close() error {
	return nil
}
