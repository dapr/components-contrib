// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package postmark

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
		m.Properties = map[string]string{"serverToken": "abc", "accountToken": "123", "emailFrom": "test1@example.net", "emailTo": "test2@example.net", "subject": "hello"}
		r := Postmark{logger: logger}
		pMeta, err := r.parseMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "abc", pMeta.ServerToken)
		assert.Equal(t, "123", pMeta.AccountToken)
		assert.Equal(t, "test1@example.net", pMeta.EmailFrom)
		assert.Equal(t, "test2@example.net", pMeta.EmailTo)
		assert.Equal(t, "hello", pMeta.Subject)
	})
}
