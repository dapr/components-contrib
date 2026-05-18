/*
Copyright 2026 The Dapr Authors
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

package git

import (
	"context"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/dapr/components-contrib/configuration"
)

// subscription holds the per-subscriber state held by the store.
//
// `ctx` is the subscriber's child context — cancelled when the subscriber
// calls Unsubscribe. Handlers receive this context so they can detect their
// own subscription being torn down (e.g. to abort a downstream gRPC stream).
type subscription struct {
	id            string
	keys          []string
	handler       configuration.UpdateHandler
	deliveredHEAD plumbing.Hash
	ctx           context.Context
	cancel        context.CancelFunc
}

// cloneItem returns a deep copy of in — fresh Value, Version, and Metadata
// map. The Metadata is allocated even when the source is empty so callers
// can mutate it safely.
func cloneItem(in *configuration.Item) *configuration.Item {
	if in == nil {
		return nil
	}
	out := &configuration.Item{
		Value:   in.Value,
		Version: in.Version,
	}
	if len(in.Metadata) == 0 {
		out.Metadata = map[string]string{}
		return out
	}
	out.Metadata = make(map[string]string, len(in.Metadata))
	for k, v := range in.Metadata {
		out.Metadata[k] = v
	}
	return out
}

// filterByKeys returns deep-cloned items containing only the requested keys.
// Callers own the returned items and may freely mutate them — the store's
// internal snapshot is never aliased.
//
// An empty keys slice means "all keys".
func filterByKeys(items map[string]*configuration.Item, keys []string) map[string]*configuration.Item {
	if len(keys) == 0 {
		out := make(map[string]*configuration.Item, len(items))
		for k, v := range items {
			out[k] = cloneItem(v)
		}
		return out
	}
	out := make(map[string]*configuration.Item, len(keys))
	for _, k := range keys {
		if v, ok := items[k]; ok {
			out[k] = cloneItem(v)
		}
	}
	return out
}
