/*
Copyright 2026 The Dapr Authors
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

package git

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/configuration"
)

func TestDiffSnapshots(t *testing.T) {
	prev := map[string]*configuration.Item{
		"a": {Value: "1"},
		"b": {Value: "2"},
		"c": {Value: "3"},
	}
	next := map[string]*configuration.Item{
		"a": {Value: "1"},
		"b": {Value: "2-changed"},
		"d": {Value: "4"},
	}
	got := diffSnapshots(prev, next, nil, "abc1234")

	assert.Len(t, got, 3)

	// Changed
	assert.Equal(t, "2-changed", got["b"].Value)
	assert.Equal(t, "abc1234", got["b"].Version)

	// Added
	assert.Equal(t, "4", got["d"].Value)

	// Deleted
	assert.Equal(t, "", got["c"].Value)
	assert.Equal(t, "true", got["c"].Metadata["deleted"])

	// Unchanged
	_, ok := got["a"]
	assert.False(t, ok)
}

func TestDiffSnapshots_KeyFilter(t *testing.T) {
	prev := map[string]*configuration.Item{
		"alpha": {Value: "1"},
		"beta":  {Value: "2"},
	}
	next := map[string]*configuration.Item{
		"alpha": {Value: "1-changed"},
		"beta":  {Value: "2-changed"},
	}
	got := diffSnapshots(prev, next, []string{"alpha"}, "v1")
	assert.Len(t, got, 1)
	assert.Equal(t, "1-changed", got["alpha"].Value)
}

func TestDiffSnapshots_LRUMiss(t *testing.T) {
	// LRU miss → prev=nil → everything in `next` is emitted as add.
	next := map[string]*configuration.Item{
		"key1": {Value: "v1"},
		"key2": {Value: "v2"},
	}
	got := diffSnapshots(nil, next, nil, "v1")
	assert.Len(t, got, 2)
	assert.Equal(t, "v1", got["key1"].Value)
	assert.Equal(t, "v2", got["key2"].Value)
}

func TestKeySet(t *testing.T) {
	all := keysAsSet(nil)
	assert.True(t, all.match("anything"))

	some := keysAsSet([]string{"x", "y"})
	assert.True(t, some.match("x"))
	assert.False(t, some.match("z"))
}
