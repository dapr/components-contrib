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
	// inFlightBytes counts bytes reserved by uploads currently being read, so
	// that maxTotalSize bounds stored and in-flight memory combined.
	inFlightBytes int64
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

	// Overwrites credit the size of the file being replaced when reserving
	// capacity, so a full store can still be updated in place. Creates pass
	// no credit: they must not subtract a file another writer adds
	// concurrently.
	var replacedFile string
	if req.Overwrite {
		replacedFile = fileName
	}

	data, err := s.readAllReserved(req.Data, replacedFile)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// The reservation ends here, one way or another: on success the bytes
	// become committed, on failure they are released.
	s.inFlightBytes -= int64(len(data))
	if !req.Overwrite {
		if _, ok := s.files[fileName]; ok {
			return binarystore.ErrFileAlreadyExists
		}
	}
	// The reservation guarantees the store has room for data; for an
	// overwrite the replaced file's size was credited during the read, and
	// its bytes are released here as it is replaced.
	existing := s.files[fileName]
	s.totalBytes += int64(len(data)) - int64(len(existing))
	s.files[fileName] = data
	return nil
}

// uploadChunkSize bounds the scratch buffer used while reading an upload.
const uploadChunkSize = 32 * 1024

// readAllReserved reads r to completion, reading at most maxSize+1 bytes and
// reserving capacity against maxTotalSize as the data arrives, so in-flight
// uploads count towards the total limit. When replacedFile is non-empty it
// names the file an overwrite will replace, and its current size is credited
// against the reserved capacity. The reservation is released when reading
// fails; on success it stays held until the caller commits or discards the
// data under the write lock.
func (s *InMemoryBinaryStore) readAllReserved(r io.Reader, replacedFile string) (data []byte, err error) {
	limit := s.maxSize + 1
	tmp := make([]byte, uploadChunkSize)
	var reserved int64
	defer func() {
		if err != nil && reserved > 0 {
			s.mu.Lock()
			s.inFlightBytes -= reserved
			s.mu.Unlock()
		}
	}()
	for int64(len(data)) < limit {
		n, readErr := r.Read(tmp[:min(int64(len(tmp)), limit-int64(len(data)))])
		if n > 0 {
			if !s.reserve(int64(n), replacedFile) {
				return nil, fmt.Errorf("%w: maximum total size is %d bytes", binarystore.ErrStoreFull, s.maxTotalSize)
			}
			reserved += int64(n)
			data = append(data, tmp[:n]...)
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, readErr
		}
	}
	if int64(len(data)) > s.maxSize {
		return nil, fmt.Errorf("%w: maximum size is %d bytes", binarystore.ErrFileTooLarge, s.maxSize)
	}
	return data, nil
}

// reserve adds n bytes to the in-flight total, provided the combined stored
// and in-flight bytes stay within maxTotalSize. When replacedFile names a
// file an overwrite will replace, its current size is excluded from the
// stored total so a full store can still be updated in place; the old copy
// is dropped when the upload commits. The size is read under the same lock
// on every call, so the credit tracks concurrent changes to that file.
func (s *InMemoryBinaryStore) reserve(n int64, replacedFile string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.totalBytes-int64(len(s.files[replacedFile]))+s.inFlightBytes+n > s.maxTotalSize {
		return false
	}
	s.inFlightBytes += n
	return true
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
	// inFlightBytes is deliberately not zeroed: uploads still in progress own
	// their reservations and subtract them when they commit or fail. Zeroing
	// it here would drive the counter negative once those uploads finish.
	return nil
}
