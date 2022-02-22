/*
Copyright 2021 The Dapr Authors
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
		return fmt.Sprintf("%s: %s", prefix, e.err)
	}

	return errors.New(prefix).Error()
}

// NewETagError returns an ETagError wrapping an existing context error.
func NewETagError(kind ETagErrorKind, err error) *ETagError {
	return &ETagError{
		err:  err,
		kind: kind,
	}
}

// BulkDeleteRowMismatchError represents mismatch in rowcount while deleting rows.
type BulkDeleteRowMismatchError struct {
	expected uint64
	affected uint64
}

func (e *BulkDeleteRowMismatchError) Error() string {
	return fmt.Sprintf("delete affected only %d rows, expected %d", e.affected, e.expected)
}

// BulkDeleteRowMismatchError returns a BulkDeleteRowMismatchError.
func NewBulkDeleteRowMismatchError(expected, affected uint64) *BulkDeleteRowMismatchError {
	return &BulkDeleteRowMismatchError{
		expected: expected,
		affected: affected,
	}
}
