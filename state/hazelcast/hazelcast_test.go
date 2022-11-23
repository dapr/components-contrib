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

package hazelcast

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
)

func TestValidateMetadata(t *testing.T) {
	t.Run("without required configuration", func(t *testing.T) {
		properties := map[string]string{}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		_, err := validateAndParseMetadata(m)
		assert.NotNil(t, err)
	})

	t.Run("without server configuration", func(t *testing.T) {
		properties := map[string]string{
			"hazelcastMap": "foo-map",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		_, err := validateAndParseMetadata(m)
		assert.NotNil(t, err)
	})

	t.Run("without map configuration", func(t *testing.T) {
		properties := map[string]string{
			"hazelcastServers": "hz1:5701",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		_, err := validateAndParseMetadata(m)
		assert.NotNil(t, err)
	})

	t.Run("with valid configuration", func(t *testing.T) {
		properties := map[string]string{
			"hazelcastServers": "hz1:5701",
			"hazelcastMap":     "foo-map",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		meta, err := validateAndParseMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, properties["hazelcastServers"], meta.HazelcastServers)
	})
}
