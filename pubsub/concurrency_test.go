// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
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
		c, _ := Concurrency(m)

		assert.Equal(t, Parallel, c)
	})

	t.Run("parallel", func(t *testing.T) {
		m := map[string]string{ConcurrencyKey: string(Parallel)}
		c, _ := Concurrency(m)

		assert.Equal(t, Parallel, c)
	})

	t.Run("single", func(t *testing.T) {
		m := map[string]string{ConcurrencyKey: string(Single)}
		c, _ := Concurrency(m)

		assert.Equal(t, Single, c)
	})

	t.Run("invalid", func(t *testing.T) {
		m := map[string]string{ConcurrencyKey: "a"}
		c, err := Concurrency(m)

		assert.Empty(t, c)
		assert.Error(t, err)
	})
}
