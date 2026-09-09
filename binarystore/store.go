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

// Package binarystore defines the BinaryStore building block interface for
// reading, writing, and deleting large named binary files. It is designed to
// handle content ranging from a few bytes to several gigabytes via streaming
// I/O, so neither the Dapr runtime nor the component needs to buffer entire
// files in memory.
//
// Authentication and connection details are configured in the component YAML;
// operations such as listing files or persisting per-file metadata are
// intentionally out of scope for this building block.
package binarystore

import (
	"context"
	"io"

	"github.com/dapr/components-contrib/metadata"
)

// BinaryStore is the interface for binary store operations.
type BinaryStore interface {
	metadata.ComponentWithMetadata

	// Init initializes the binary store with the provided metadata.
	Init(ctx context.Context, metadata Metadata) error

	// Features returns the list of optional features supported by this implementation.
	Features() []Feature

	// Set stores binary data identified by req.FileName.
	//
	// When req.Overwrite is false (POST semantics), Set returns ErrFileAlreadyExists
	// if a file with that name already exists. When req.Overwrite is true (PUT
	// semantics), any existing file is replaced.
	//
	// Set reads req.Data to completion; the caller must not close it until Set
	// returns.
	Set(ctx context.Context, req *SetRequest) error

	// Get retrieves the binary data for the file identified by req.FileName.
	//
	// The Data field of the returned GetResponse is a streaming reader; the
	// caller is responsible for closing it after reading. If the file does not
	// exist, ErrFileNotFound is returned.
	Get(ctx context.Context, req *GetRequest) (*GetResponse, error)

	// Delete removes the file identified by req.FileName. If the file does not
	// exist, ErrFileNotFound is returned.
	Delete(ctx context.Context, req *DeleteRequest) error

	io.Closer
}
