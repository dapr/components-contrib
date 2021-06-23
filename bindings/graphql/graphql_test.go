// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package graphql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOperations(t *testing.T) {
	t.Parallel()
	t.Run("Get operation list", func(t *testing.T) {
		t.Parallel()
		b := NewGraphQL(nil)
		assert.NotNil(t, b)
		l := b.Operations()
		assert.Equal(t, 2, len(l))
	})
}
