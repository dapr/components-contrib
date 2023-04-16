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

package ratelimit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/middleware"
)

func TestMiddlewareGetNativeMetadata(t *testing.T) {
	m := &Middleware{}

	t.Run(maxRequestsPerSecondKey+" is 0", func(t *testing.T) {
		res, err := m.getNativeMetadata(middleware.Metadata{Base: metadata.Base{Properties: map[string]string{
			maxRequestsPerSecondKey: "0",
		}}})
		require.Error(t, err)
		assert.ErrorContains(t, err, "metadata property "+maxRequestsPerSecondKey+" must be a positive value")
		assert.Nil(t, res)
	})

	t.Run(maxRequestsPerSecondKey+" is negative", func(t *testing.T) {
		res, err := m.getNativeMetadata(middleware.Metadata{Base: metadata.Base{Properties: map[string]string{
			maxRequestsPerSecondKey: "-2",
		}}})
		require.Error(t, err)
		assert.ErrorContains(t, err, "metadata property "+maxRequestsPerSecondKey+" must be a positive value")
		assert.Nil(t, res)
	})

	t.Run(maxRequestsPerSecondKey+" is invalid", func(t *testing.T) {
		res, err := m.getNativeMetadata(middleware.Metadata{Base: metadata.Base{Properties: map[string]string{
			maxRequestsPerSecondKey: "foo-bar",
		}}})
		require.Error(t, err)
		assert.ErrorContains(t, err, "cannot parse 'MaxRequestsPerSecond' as float")
		assert.Nil(t, res)
	})

	t.Run(maxRequestsPerSecondKey+" is an integer", func(t *testing.T) {
		res, err := m.getNativeMetadata(middleware.Metadata{Base: metadata.Base{Properties: map[string]string{
			maxRequestsPerSecondKey: "10",
		}}})
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, float64(10), res.MaxRequestsPerSecond)
	})

	t.Run(maxRequestsPerSecondKey+" is a float", func(t *testing.T) {
		res, err := m.getNativeMetadata(middleware.Metadata{Base: metadata.Base{Properties: map[string]string{
			maxRequestsPerSecondKey: "42.42",
		}}})
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, float64(42.42), res.MaxRequestsPerSecond)
	})
}
