// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package sendgrid

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func TestParseMetadata(t *testing.T) {
	logger := logger.NewLogger("test")

	t.Run("Has correct metadata", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"apiKey": "123", "emailFrom": "test1@example.net", "emailTo": "test2@example.net", "subject": "hello"}
		r := SendGrid{logger: logger}
		sgMeta, err := r.parseMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "123", sgMeta.APIKey)
		assert.Equal(t, "test1@example.net", sgMeta.EmailFrom)
		assert.Equal(t, "test2@example.net", sgMeta.EmailTo)
		assert.Equal(t, "hello", sgMeta.Subject)
	})
}
