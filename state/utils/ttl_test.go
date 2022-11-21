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
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTTL(t *testing.T) {
	t.Run("TTL Not an integer", func(t *testing.T) {
		ttlInSeconds := "not an integer"
		ttl, err := ParseTTL(map[string]string{
			MetadataTTLKey: ttlInSeconds,
		})
		require.Error(t, err)
		assert.Nil(t, ttl)
	})

	t.Run("TTL specified with wrong key", func(t *testing.T) {
		ttlInSeconds := 12345
		ttl, err := ParseTTL(map[string]string{
			"expirationTime": strconv.Itoa(ttlInSeconds),
		})
		require.NoError(t, err)
		assert.Nil(t, ttl)
	})

	t.Run("TTL is a number", func(t *testing.T) {
		ttlInSeconds := 12345
		ttl, err := ParseTTL(map[string]string{
			MetadataTTLKey: strconv.Itoa(ttlInSeconds),
		})
		require.NoError(t, err)
		assert.Equal(t, *ttl, ttlInSeconds)
	})

	t.Run("TTL not set", func(t *testing.T) {
		ttl, err := ParseTTL(map[string]string{})
		require.NoError(t, err)
		assert.Nil(t, ttl)
	})

	t.Run("TTL < -1", func(t *testing.T) {
		ttl, err := ParseTTL(map[string]string{
			MetadataTTLKey: "-3",
		})
		require.Error(t, err)
		assert.Nil(t, ttl)
	})

	t.Run("TTL bigger than 32-bit", func(t *testing.T) {
		ttl, err := ParseTTL(map[string]string{
			MetadataTTLKey: strconv.FormatInt(math.MaxInt32+1, 10),
		})
		fmt.Println(err)
		require.Error(t, err)
		assert.Nil(t, ttl)
	})
}
