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

// Package inmemory provides an in-memory BinaryStore implementation. Data is
// held only in process memory: it is not persisted to disk and does not
// survive a process restart, and it is not shared across multiple instances
// of an application. It is useful for local development, testing, and
// demos where a durable object storage backend is not required.
package inmemory

import (
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/dapr/components-contrib/binarystore"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// InMemoryBinaryStore is an in-memory BinaryStore implementation.
type InMemoryBinaryStore struct {
	logger logger.Logger
	mu     sync.RWMutex
	files  map[string][]byte
	prefix string
}

// NewInMemoryBinaryStore returns a new in-memory BinaryStore.
func NewInMemoryBinaryStore(log logger.Logger) binarystore.BinaryStore {
	return &InMemoryBinaryStore{
		logger: log,
		files:  make(map[string][]byte),
	}
}

func (s *InMemoryBinaryStore) Init(_ context.Context, md binarystore.Metadata) error {
	s.prefix = md.Properties["prefix"]
	return nil
}

func (s *InMemoryBinaryStore) Features() []binarystore.Feature {
	return []binarystore.Feature{}
}

func (s *InMemoryBinaryStore) Set(_ context.Context, req *binarystore.SetRequest) error {
	if req.FileName == "" {
		return binarystore.ErrMissingFileName
	}

	data, err := io.ReadAll(req.Data)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	fileName := binarystore.ObjectPath(s.prefix, req.FileName)
	if !req.Overwrite {
		if _, ok := s.files[fileName]; ok {
			return binarystore.ErrFileAlreadyExists
		}
	}
	s.files[fileName] = data
	return nil
}

func (s *InMemoryBinaryStore) Get(_ context.Context, req *binarystore.GetRequest) (*binarystore.GetResponse, error) {
	if req.FileName == "" {
		return nil, binarystore.ErrMissingFileName
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.files[binarystore.ObjectPath(s.prefix, req.FileName)]
	if !ok {
		return nil, binarystore.ErrFileNotFound
	}
	return &binarystore.GetResponse{Data: io.NopCloser(bytes.NewReader(data))}, nil
}

func (s *InMemoryBinaryStore) Delete(_ context.Context, req *binarystore.DeleteRequest) error {
	if req.FileName == "" {
		return binarystore.ErrMissingFileName
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	fileName := binarystore.ObjectPath(s.prefix, req.FileName)
	if _, ok := s.files[fileName]; !ok {
		return binarystore.ErrFileNotFound
	}
	delete(s.files, fileName)
	return nil
}

func (s *InMemoryBinaryStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	// no metadata, hence no metadata struct to convert here
	return
}

func (s *InMemoryBinaryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k := range s.files {
		delete(s.files, k)
	}
	return nil
}
