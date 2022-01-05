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

package utils

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHasOperation(t *testing.T) {
	t.Run("all operations", func(t *testing.T) {
		cc := CommonConfig{
			ComponentType: "state",
			ComponentName: "redis",
			AllOperations: true,
		}
		assert.True(t, cc.HasOperation("op"))
	})
	t.Run("operations list", func(t *testing.T) {
		cc := CommonConfig{
			ComponentType: "state",
			ComponentName: "redis",
			Operations:    NewStringSet("op1", "op2"),
		}
		assert.True(t, cc.HasOperation("op1"))
		assert.True(t, cc.HasOperation("op2"))
		assert.False(t, cc.HasOperation("op3"))
	})
}

func TestCopyMap(t *testing.T) {
	cc := CommonConfig{
		ComponentType: "state",
		ComponentName: "redis",
		AllOperations: true,
	}
	in := map[string]string{
		"k": "v",
		"v": "k",
	}
	out := cc.CopyMap(in)
	assert.Equal(t, in, out)
	assert.NotEqual(t, reflect.ValueOf(in).Pointer(), reflect.ValueOf(out).Pointer())
}
