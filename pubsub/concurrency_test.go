// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConcurrency(t *testing.T) {
	t.Run("default parallel", func(t *testing.T) {
		m := map[string]string{}
		c := Concurrency(m)

		assert.Equal(t, Parallel, c)
	})

	t.Run("parallel", func(t *testing.T) {
		m := map[string]string{ConcurrencyKey: string(Parallel)}
		c := Concurrency(m)

		assert.Equal(t, Parallel, c)
	})

	t.Run("single", func(t *testing.T) {
		m := map[string]string{ConcurrencyKey: string(Single)}
		c := Concurrency(m)

		assert.Equal(t, Single, c)
	})
}
