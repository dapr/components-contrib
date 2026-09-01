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

// Package fake provides an in-memory BinaryStore implementation used only for
// tests. It is the binarystore equivalent of conversation/echo.
package fake

import (
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/dapr/components-contrib/binarystore"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// Fake is an in-memory binary store intended for tests only.
type Fake struct {
	logger logger.Logger
	mu     sync.RWMutex
	files  map[string][]byte
}

// NewFake returns a new in-memory fake BinaryStore.
func NewFake(log logger.Logger) binarystore.BinaryStore {
	return &Fake{
		logger: log,
		files:  make(map[string][]byte),
	}
}

func (f *Fake) Init(_ context.Context, _ binarystore.Metadata) error {
	return nil
}

func (f *Fake) Features() []binarystore.Feature {
	return []binarystore.Feature{}
}

func (f *Fake) Set(_ context.Context, req *binarystore.SetRequest) error {
	if req.FileName == "" {
		return binarystore.ErrMissingFileName
	}

	data, err := io.ReadAll(req.Data)
	if err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if !req.Overwrite {
		if _, ok := f.files[req.FileName]; ok {
			return binarystore.ErrFileAlreadyExists
		}
	}
	f.files[req.FileName] = data
	return nil
}

func (f *Fake) Get(_ context.Context, req *binarystore.GetRequest) (*binarystore.GetResponse, error) {
	if req.FileName == "" {
		return nil, binarystore.ErrMissingFileName
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	data, ok := f.files[req.FileName]
	if !ok {
		return nil, binarystore.ErrFileNotFound
	}
	return &binarystore.GetResponse{Data: io.NopCloser(bytes.NewReader(data))}, nil
}

func (f *Fake) Delete(_ context.Context, req *binarystore.DeleteRequest) error {
	if req.FileName == "" {
		return binarystore.ErrMissingFileName
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.files[req.FileName]; !ok {
		return binarystore.ErrFileNotFound
	}
	delete(f.files, req.FileName)
	return nil
}

func (f *Fake) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	return
}

func (f *Fake) Close() error {
	return nil
}
