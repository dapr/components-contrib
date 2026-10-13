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

// Package inmemory provides an in-memory BinaryStore implementation. Data is
// held only in process memory: it is not persisted to disk and does not
// survive a process restart, and it is not shared across multiple instances
// of an application. It is useful for local development, testing, and
// demos where a durable object storage backend is not required.
package inmemory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/dapr/components-contrib/binarystore"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	defaultMaxSize int64 = 4 * 1024 * 1024
	// defaultMaxTotalSize bounds the store to 512 MiB in total. The component
	// is intended for local development and testing, so a low ceiling is
	// preferred over risking the sidecar running out of memory.
	defaultMaxTotalSize int64 = 512 * 1024 * 1024
)

// inMemoryMetadata contains the parsed metadata for the in-memory store.
type inMemoryMetadata struct {
	Prefix string `json:"prefix" mapstructure:"prefix"`
	// MaxSize is the maximum file size, as a byte size quantity (e.g. "4Mi").
	MaxSize kitmd.ByteSize `json:"maxSize" mapstructure:"maxSize"`
	// MaxTotalSize is the maximum combined size of all files held by the
	// store, as a byte size quantity (e.g. "512Mi").
	MaxTotalSize kitmd.ByteSize `json:"maxTotalSize" mapstructure:"maxTotalSize"`
}

// InMemoryBinaryStore is an in-memory BinaryStore implementation.
type InMemoryBinaryStore struct {
	logger       logger.Logger
	mu           sync.RWMutex
	files        map[string][]byte
	prefix       string
	maxSize      int64
	maxTotalSize int64
	totalBytes   int64
}

// NewInMemoryBinaryStore returns a new in-memory BinaryStore.
func NewInMemoryBinaryStore(log logger.Logger) binarystore.BinaryStore {
	return &InMemoryBinaryStore{
		logger: log,
		files:  make(map[string][]byte),
	}
}

func (s *InMemoryBinaryStore) Init(_ context.Context, md binarystore.Metadata) error {
	// Decoded through the metadata decoder so that key matching is
	// case-insensitive and consistent with the other binary store providers.
	m := inMemoryMetadata{
		MaxSize:      kitmd.NewByteSize(defaultMaxSize),
		MaxTotalSize: kitmd.NewByteSize(defaultMaxTotalSize),
	}
	if err := kitmd.DecodeMetadata(md.Properties, &m); err != nil {
		return fmt.Errorf("failed to decode metadata: %w", err)
	}
	maxSize, err := m.MaxSize.GetBytes()
	if err != nil {
		return fmt.Errorf("invalid value for maxSize: %w", err)
	}
	maxTotalSize, err := m.MaxTotalSize.GetBytes()
	if err != nil {
		return fmt.Errorf("invalid value for maxTotalSize: %w", err)
	}
	// ByteSize is backed by resource.Quantity, which cannot represent values
	// near math.MaxInt64; such inputs already failed in GetBytes above, so
	// maxSize+1 (used to detect oversized files) and maxTotalSize+maxSize
	// (used by the capacity check) cannot overflow int64.
	if maxSize <= 0 {
		return errors.New("maxSize must be greater than zero")
	}
	if maxTotalSize <= 0 {
		return errors.New("maxTotalSize must be greater than zero")
	}
	s.prefix = m.Prefix
	s.maxSize = maxSize
	s.maxTotalSize = maxTotalSize
	return nil
}

func (s *InMemoryBinaryStore) Features() []binarystore.Feature {
	return []binarystore.Feature{}
}

func (s *InMemoryBinaryStore) Set(_ context.Context, req *binarystore.SetRequest) error {
	if req.FileName == "" {
		return binarystore.ErrMissingFileName
	}
	// A nil body is stored as an empty file rather than panicking in ReadAll.
	if req.Data == nil {
		req.Data = bytes.NewReader(nil)
	}

	// Validate the name before reading the body so that requests that will be
	// rejected do not pay for the read and allocation. ObjectPath needs no
	// lock.
	fileName, err := binarystore.ObjectPath(s.prefix, req.FileName)
	if err != nil {
		return err
	}

	if !req.Overwrite {
		// Cheap early rejection of duplicates; the authoritative check runs
		// again below under the write lock.
		s.mu.RLock()
		_, ok := s.files[fileName]
		s.mu.RUnlock()
		if ok {
			return binarystore.ErrFileAlreadyExists
		}
	}

	data, err := io.ReadAll(io.LimitReader(req.Data, s.maxSize+1))
	if err != nil {
		return err
	}
	if int64(len(data)) > s.maxSize {
		return fmt.Errorf("%w: maximum size is %d bytes", binarystore.ErrFileTooLarge, s.maxSize)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !req.Overwrite {
		if _, ok := s.files[fileName]; ok {
			return binarystore.ErrFileAlreadyExists
		}
	}
	existing := s.files[fileName]
	if s.totalBytes-int64(len(existing))+int64(len(data)) > s.maxTotalSize {
		return fmt.Errorf("%w: maximum total size is %d bytes", binarystore.ErrStoreFull, s.maxTotalSize)
	}
	s.totalBytes += int64(len(data)) - int64(len(existing))
	s.files[fileName] = data
	return nil
}

func (s *InMemoryBinaryStore) Get(_ context.Context, req *binarystore.GetRequest) (*binarystore.GetResponse, error) {
	if req.FileName == "" {
		return nil, binarystore.ErrMissingFileName
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	fileName, err := binarystore.ObjectPath(s.prefix, req.FileName)
	if err != nil {
		return nil, err
	}
	data, ok := s.files[fileName]
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

	fileName, err := binarystore.ObjectPath(s.prefix, req.FileName)
	if err != nil {
		return err
	}
	data, ok := s.files[fileName]
	if !ok {
		return binarystore.ErrFileNotFound
	}
	delete(s.files, fileName)
	s.totalBytes -= int64(len(data))
	return nil
}

func (s *InMemoryBinaryStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := inMemoryMetadata{}
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BinaryStoreType)
	return
}

func (s *InMemoryBinaryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k := range s.files {
		delete(s.files, k)
	}
	s.totalBytes = 0
	return nil
}
