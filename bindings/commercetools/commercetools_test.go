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
		m.Properties = map[string]string{"Region": "a", "Provider": "a", "ProjectKey": "a", "ClientID": "a", "ClientSecret": "a", "Scopes": "a"}
		k := Binding{logger: logger}
		meta, err := k.getCommercetoolsMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "a", meta.Region)
		assert.Equal(t, "a", meta.Provider)
		assert.Equal(t, "a", meta.ProjectKey)
		assert.Equal(t, "a", meta.ClientID)
		assert.Equal(t, "a", meta.ClientSecret)
		assert.Equal(t, "a", meta.Scopes)
	})
}
