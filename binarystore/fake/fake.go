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

// Package fake provides a fake BinaryStore implementation for tests of
// components and runtimes that consume a binary store. It is not a user-facing
// component and so it does not ship component metadata.
//
// Following the convention used elsewhere in Dapr, Fake holds one function
// field per interface method and exposes a WithXxx builder for each, so a test
// can make an individual operation fail on demand:
//
//	store := fake.New().WithGet(func(context.Context, *binarystore.GetRequest) (*binarystore.GetResponse, error) {
//		return nil, errors.New("boom")
//	})
//
// Every field defaults to the in-memory binary store, so a Fake that overrides
// nothing behaves like a real store while holding all data in process memory.
// That makes it usable both for wiring tests that just need a working store and
// for exercising a consumer's error handling.
package fake

import (
	"context"

	"github.com/dapr/components-contrib/binarystore"
	inmemory "github.com/dapr/components-contrib/binarystore/in-memory"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// Fake is a configurable fake implementation of binarystore.BinaryStore.
type Fake struct {
	fnInit                 func(ctx context.Context, metadata binarystore.Metadata) error
	fnFeatures             func() []binarystore.Feature
	fnSet                  func(ctx context.Context, req *binarystore.SetRequest) error
	fnGet                  func(ctx context.Context, req *binarystore.GetRequest) (*binarystore.GetResponse, error)
	fnDelete               func(ctx context.Context, req *binarystore.DeleteRequest) error
	fnGetComponentMetadata func() metadata.MetadataMap
	fnClose                func() error
}

var _ binarystore.BinaryStore = (*Fake)(nil)

// New returns a Fake whose operations are all backed by the in-memory binary
// store. Use the WithXxx builders to override individual operations.
func New() *Fake {
	backing := inmemory.NewInMemoryBinaryStore(logger.NewLogger("fake.binarystore"))

	f := &Fake{
		fnInit:     backing.Init,
		fnFeatures: backing.Features,
		fnSet:      backing.Set,
		fnGet:      backing.Get,
		fnDelete:   backing.Delete,
		fnClose:    backing.Close,
	}

	// GetComponentMetadata is only part of metadata.ComponentWithMetadata under
	// the `metadata` build tag, so it is resolved dynamically rather than
	// through the binarystore.BinaryStore interface.
	if cm, ok := backing.(interface {
		GetComponentMetadata() metadata.MetadataMap
	}); ok {
		f.fnGetComponentMetadata = cm.GetComponentMetadata
	} else {
		f.fnGetComponentMetadata = func() metadata.MetadataMap { return metadata.MetadataMap{} }
	}

	return f
}

func (f *Fake) WithInit(fn func(ctx context.Context, metadata binarystore.Metadata) error) *Fake {
	f.fnInit = fn
	return f
}

func (f *Fake) WithFeatures(fn func() []binarystore.Feature) *Fake {
	f.fnFeatures = fn
	return f
}

func (f *Fake) WithSet(fn func(ctx context.Context, req *binarystore.SetRequest) error) *Fake {
	f.fnSet = fn
	return f
}

func (f *Fake) WithGet(fn func(ctx context.Context, req *binarystore.GetRequest) (*binarystore.GetResponse, error)) *Fake {
	f.fnGet = fn
	return f
}

func (f *Fake) WithDelete(fn func(ctx context.Context, req *binarystore.DeleteRequest) error) *Fake {
	f.fnDelete = fn
	return f
}

func (f *Fake) WithGetComponentMetadata(fn func() metadata.MetadataMap) *Fake {
	f.fnGetComponentMetadata = fn
	return f
}

func (f *Fake) WithClose(fn func() error) *Fake {
	f.fnClose = fn
	return f
}

func (f *Fake) Init(ctx context.Context, md binarystore.Metadata) error {
	return f.fnInit(ctx, md)
}

func (f *Fake) Features() []binarystore.Feature {
	return f.fnFeatures()
}

func (f *Fake) Set(ctx context.Context, req *binarystore.SetRequest) error {
	return f.fnSet(ctx, req)
}

func (f *Fake) Get(ctx context.Context, req *binarystore.GetRequest) (*binarystore.GetResponse, error) {
	return f.fnGet(ctx, req)
}

func (f *Fake) Delete(ctx context.Context, req *binarystore.DeleteRequest) error {
	return f.fnDelete(ctx, req)
}

func (f *Fake) GetComponentMetadata() metadata.MetadataMap {
	return f.fnGetComponentMetadata()
}

func (f *Fake) Close() error {
	return f.fnClose()
}
