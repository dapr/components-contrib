// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

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
