/*
Copyright 2025 The Dapr Authors
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

// Package fake provides a fake BinaryStore implementation intended for tests
// and local development of components and runtimes that consume a binary
// store. It is backed by the in-memory binary store, so it behaves like a real
// store while holding all data in process memory. It is not a user-facing
// component and so it does not ship component metadata.
package fake

import (
	"github.com/dapr/components-contrib/binarystore"
	inmemory "github.com/dapr/components-contrib/binarystore/in-memory"
	"github.com/dapr/kit/logger"
)

// NewFake returns a fake BinaryStore backed by the in-memory implementation.
func NewFake(log logger.Logger) binarystore.BinaryStore {
	return inmemory.NewInMemoryBinaryStore(log)
}
