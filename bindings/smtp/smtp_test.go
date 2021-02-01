// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package smtp

import (
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	logger := logger.NewLogger("test")

	t.Run("Has correct metadata", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"host":          "mailserver.dapr.io",
			"port":          "25",
			"user":          "user@dapr.io",
			"password":      "P@$$w0rd!",
			"skipTLSVerify": "true",
			"emailFrom":     "from@dapr.io",
			"emailTo":       "to@dapr.io",
			"emailCC":       "cc@dapr.io",
			"emailBCC":      "bcc@dapr.io",
			"subject":       "Test email"}
		r := Mailer{logger: logger}
		smtpMeta, err := r.parseMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "mailserver.dapr.io", smtpMeta.Host)
		assert.Equal(t, "25", smtpMeta.Port)
		assert.Equal(t, "user@dapr.io", smtpMeta.User)
		assert.Equal(t, "P@$$w0rd!", smtpMeta.Password)
		assert.Equal(t, true, smtpMeta.SkipTLSVerify)
		assert.Equal(t, "from@dapr.io", smtpMeta.EmailFrom)
		assert.Equal(t, "to@dapr.io", smtpMeta.EmailTo)
		assert.Equal(t, "cc@dapr.io", smtpMeta.EmailCC)
		assert.Equal(t, "bcc@dapr.io", smtpMeta.EmailBCC)
		assert.Equal(t, "Test email", smtpMeta.Subject)
	})
}

func TestMergeWithRequestMetadata(t *testing.T) {
	t.Run("Has merged metadata", func(t *testing.T) {
		var smtpMeta = Metadata{
			Host:          "mailserver.dapr.io",
			Port:          "25",
			User:          "user@dapr.io",
			SkipTLSVerify: true,
			Password:      "P@$$w0rd!",
			EmailFrom:     "from@dapr.io",
			EmailTo:       "to@dapr.io",
			EmailCC:       "cc@dapr.io",
			EmailBCC:      "bcc@dapr.io",
			Subject:       "Test email"}

		var request = bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"emailFrom": "req-from@dapr.io",
			"emailTo":   "req-to@dapr.io",
			"emailCC":   "req-cc@dapr.io",
			"emailBCC":  "req-bcc@dapr.io",
			"subject":   "req-Test email"}

		mergedMeta := smtpMeta.mergeWithRequestMetadata(&request)

		assert.Equal(t, "mailserver.dapr.io", mergedMeta.Host)
		assert.Equal(t, "25", mergedMeta.Port)
		assert.Equal(t, "user@dapr.io", mergedMeta.User)
		assert.Equal(t, "P@$$w0rd!", mergedMeta.Password)
		assert.Equal(t, true, mergedMeta.SkipTLSVerify)
		assert.Equal(t, "req-from@dapr.io", mergedMeta.EmailFrom)
		assert.Equal(t, "req-to@dapr.io", mergedMeta.EmailTo)
		assert.Equal(t, "req-cc@dapr.io", mergedMeta.EmailCC)
		assert.Equal(t, "req-bcc@dapr.io", mergedMeta.EmailBCC)
		assert.Equal(t, "req-Test email", mergedMeta.Subject)
	})
}
