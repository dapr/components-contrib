// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package state

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestETagError(t *testing.T) {
	t.Run("invalid with context error", func(t *testing.T) {
		cerr := errors.New("error1")
		err := NewETagError(ETagInvalid, cerr)

		assert.Equal(t, invalidPrefix+": error1", err.Error())
	})

	t.Run("invalid without context error", func(t *testing.T) {
		err := NewETagError(ETagInvalid, nil)

		assert.Equal(t, invalidPrefix, err.Error())
	})

	t.Run("mismatch with context error", func(t *testing.T) {
		cerr := errors.New("error1")
		err := NewETagError(ETagMismatch, cerr)

		assert.Equal(t, mismatchPrefix+": error1", err.Error())
	})

	t.Run("mismatch without context error", func(t *testing.T) {
		err := NewETagError(ETagMismatch, nil)

		assert.Equal(t, mismatchPrefix, err.Error())
	})

	t.Run("valid kind", func(t *testing.T) {
		err := NewETagError(ETagMismatch, nil)

		assert.IsType(t, ETagMismatch, err.kind)
	})
}
