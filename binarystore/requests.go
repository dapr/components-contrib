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

package binarystore

import (
	"errors"
	"io"
)

// Sentinel errors returned by BinaryStore implementations.
var (
	// ErrFileAlreadyExists is returned by Set when Overwrite is false and a
	// file with the same name already exists.
	ErrFileAlreadyExists = errors.New("file already exists")

	// ErrFileNotFound is returned by Get and Delete when the named file does
	// not exist.
	ErrFileNotFound = errors.New("file not found")

	// ErrMissingFileName is returned when a required FileName field is empty.
	ErrMissingFileName = errors.New("file name is required")
)

// SetRequest is the request object for the Set operation.
type SetRequest struct {
	// FileName is the identifier of the file to store. Must not be empty.
	FileName string

	// Data is the binary content to store. It is consumed as a stream; the
	// provider reads it to completion. The caller must not close it until Set
	// returns.
	Data io.Reader

	// Overwrite controls create-vs-upsert semantics.
	//
	// false — POST semantics: return ErrFileAlreadyExists if the file exists.
	// true  — PUT semantics: create the file or replace it if it already exists.
	Overwrite bool
}

// GetRequest is the request object for the Get operation.
type GetRequest struct {
	// FileName is the identifier of the file to retrieve. Must not be empty.
	FileName string
}

// GetResponse is the response object for the Get operation.
type GetResponse struct {
	// Data is the binary content of the file as a streaming reader. The caller
	// is responsible for closing Data after reading.
	Data io.ReadCloser
}

// DeleteRequest is the request object for the Delete operation.
type DeleteRequest struct {
	// FileName is the identifier of the file to delete. Must not be empty.
	FileName string
}
