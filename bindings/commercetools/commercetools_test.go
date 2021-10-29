// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

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
		m.Properties = map[string]string{"region": "a", "provider": "a", "projectKey": "a", "clientID": "a", "clientSecret": "a", "scopes": "a"}
		k := Binding{logger: logger}
		meta, err := k.getCommercetoolsMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "a", meta.region)
		assert.Equal(t, "a", meta.provider)
		assert.Equal(t, "a", meta.projectKey)
		assert.Equal(t, "a", meta.clientID)
		assert.Equal(t, "a", meta.clientSecret)
		assert.Equal(t, "a", meta.scopes)
	})
}
