/*
Copyright 2023 The Dapr Authors
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

package state

import (
	"errors"
	"fmt"
)

type ETagErrorKind string

const (
	mismatchPrefix = "possible etag mismatch. error from state store"
	invalidPrefix  = "invalid etag value"

	ETagInvalid  ETagErrorKind = "invalid"
	ETagMismatch ETagErrorKind = "mismatch"
)

// ETagError is a custom error type for etag exceptions.
type ETagError struct {
	err  error
	kind ETagErrorKind
}

// NewETagError returns an ETagError wrapping an existing context error.
func NewETagError(kind ETagErrorKind, err error) *ETagError {
	return &ETagError{
		err:  err,
		kind: kind,
	}
}

func (e *ETagError) Kind() ETagErrorKind {
	return e.kind
}

func (e *ETagError) Error() string {
	var prefix string
	switch e.kind {
	case ETagInvalid:
		prefix = invalidPrefix
	case ETagMismatch:
		prefix = mismatchPrefix
	}

	if e.err != nil {
		return prefix + ": " + e.err.Error()
	}

	return prefix
}

func (e *ETagError) Unwrap() error {
	return e.err
}

// BulkDeleteRowMismatchError represents mismatch in rowcount while deleting rows.
type BulkDeleteRowMismatchError struct {
	expected uint64
	affected uint64
}

// BulkDeleteRowMismatchError returns a BulkDeleteRowMismatchError.
func NewBulkDeleteRowMismatchError(expected, affected uint64) *BulkDeleteRowMismatchError {
	return &BulkDeleteRowMismatchError{
		expected: expected,
		affected: affected,
	}
}

func (e *BulkDeleteRowMismatchError) Error() string {
	return fmt.Sprintf("delete affected only %d rows, expected %d", e.affected, e.expected)
}

// BulkStoreError is an error object that contains details on the operations that failed.
type BulkStoreError struct {
	key string
	err error
}

func NewBulkStoreError(key string, err error) BulkStoreError {
	return BulkStoreError{
		key: key,
		err: err,
	}
}

// Key returns the key of the operation that failed.
func (e BulkStoreError) Key() string {
	return e.key
}

// Error returns the error message. It implements the error interface.
func (e BulkStoreError) Error() string {
	return e.err.Error()
}

// Unwrap returns the wrapped error. It implements the error wrapping interface.
func (e BulkStoreError) Unwrap() error {
	return e.err
}

// ETagError returns an *ETagError if the wrapped error is of that kind; otherwise, returns nil
func (e BulkStoreError) ETagError() *ETagError {
	var etagErr *ETagError
	if errors.As(e.err, &etagErr) {
		return etagErr
	}
	return nil
}
