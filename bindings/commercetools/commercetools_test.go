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

package commercetools

import (
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"

	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	logger := logger.NewLogger("test")

	t.Run("correct metadata", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"region": "a", "provider": "a", "projectKey": "a", "clientID": "a", "clientSecret": "a", "scopes": "b"}
		k := Binding{logger: logger}
		meta, err := k.getCommercetoolsMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "a", meta.region)
		assert.Equal(t, "a", meta.provider)
		assert.Equal(t, "a", meta.projectKey)
		assert.Equal(t, "a", meta.clientID)
		assert.Equal(t, "a", meta.clientSecret)
		assert.Equal(t, "b", meta.scopes)
	})
}
