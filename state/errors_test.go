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
