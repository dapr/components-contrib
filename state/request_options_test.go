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
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestCheckRequestOptions is used to validate request options.
func TestCheckRequestOptions(t *testing.T) {
	t.Run("set state options", func(t *testing.T) {
		ro := SetStateOption{Concurrency: FirstWrite, Consistency: Eventual}
		err := CheckRequestOptions(ro)
		assert.NoError(t, err)
	})
	t.Run("delete state options", func(t *testing.T) {
		ro := DeleteStateOption{Concurrency: FirstWrite, Consistency: Eventual}
		err := CheckRequestOptions(ro)
		assert.NoError(t, err)
	})
	t.Run("get state options", func(t *testing.T) {
		ro := GetStateOption{Consistency: Eventual}
		err := CheckRequestOptions(ro)
		assert.NoError(t, err)
	})
	t.Run("invalid state options", func(t *testing.T) {
		ro := SetStateOption{Concurrency: "invalid", Consistency: Eventual}
		err := CheckRequestOptions(ro)
		assert.Error(t, err)
	})
}
