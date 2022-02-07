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
