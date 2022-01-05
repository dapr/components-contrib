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

package smtp

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func TestParseMetadata(t *testing.T) {
	logger := logger.NewLogger("test")

	t.Run("Has correct metadata (default priority)", func(t *testing.T) {
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
			"subject":       "Test email",
		}
		r := Mailer{logger: logger}
		smtpMeta, err := r.parseMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "mailserver.dapr.io", smtpMeta.Host)
		assert.Equal(t, 25, smtpMeta.Port)
		assert.Equal(t, "user@dapr.io", smtpMeta.User)
		assert.Equal(t, "P@$$w0rd!", smtpMeta.Password)
		assert.Equal(t, true, smtpMeta.SkipTLSVerify)
		assert.Equal(t, "from@dapr.io", smtpMeta.EmailFrom)
		assert.Equal(t, "to@dapr.io", smtpMeta.EmailTo)
		assert.Equal(t, "cc@dapr.io", smtpMeta.EmailCC)
		assert.Equal(t, "bcc@dapr.io", smtpMeta.EmailBCC)
		assert.Equal(t, "Test email", smtpMeta.Subject)
		assert.Equal(t, 3, smtpMeta.Priority)
	})
	t.Run("Has correct metadata (no default value for priority)", func(t *testing.T) {
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
			"subject":       "Test email",
			"priority":      "1",
		}
		r := Mailer{logger: logger}
		smtpMeta, err := r.parseMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "mailserver.dapr.io", smtpMeta.Host)
		assert.Equal(t, 25, smtpMeta.Port)
		assert.Equal(t, "user@dapr.io", smtpMeta.User)
		assert.Equal(t, "P@$$w0rd!", smtpMeta.Password)
		assert.Equal(t, true, smtpMeta.SkipTLSVerify)
		assert.Equal(t, "from@dapr.io", smtpMeta.EmailFrom)
		assert.Equal(t, "to@dapr.io", smtpMeta.EmailTo)
		assert.Equal(t, "cc@dapr.io", smtpMeta.EmailCC)
		assert.Equal(t, "bcc@dapr.io", smtpMeta.EmailBCC)
		assert.Equal(t, "Test email", smtpMeta.Subject)
		assert.Equal(t, 1, smtpMeta.Priority)
	})
	t.Run("Incorrrect  metadata (invalid priority)", func(t *testing.T) {
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
			"subject":       "Test email",
			"priority":      "0",
		}
		r := Mailer{logger: logger}
		smtpMeta, err := r.parseMetadata(m)
		assert.NotNil(t, smtpMeta)
		assert.NotNil(t, err)
	})
	t.Run("Incorrrect  metadata (user, no password)", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"host":          "mailserver.dapr.io",
			"port":          "25",
			"user":          "user@dapr.io",
			"skipTLSVerify": "true",
			"emailFrom":     "from@dapr.io",
			"emailTo":       "to@dapr.io",
			"emailCC":       "cc@dapr.io",
			"emailBCC":      "bcc@dapr.io",
			"subject":       "Test email",
			"priority":      "0",
		}
		r := Mailer{logger: logger}
		smtpMeta, err := r.parseMetadata(m)
		assert.NotNil(t, smtpMeta)
		assert.NotNil(t, err)
	})
	t.Run("Incorrrect  metadata (no user, password)", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"host":          "mailserver.dapr.io",
			"port":          "25",
			"password":      "P@$$w0rd!",
			"skipTLSVerify": "true",
			"emailFrom":     "from@dapr.io",
			"emailTo":       "to@dapr.io",
			"emailCC":       "cc@dapr.io",
			"emailBCC":      "bcc@dapr.io",
			"subject":       "Test email",
			"priority":      "0",
		}
		r := Mailer{logger: logger}
		smtpMeta, err := r.parseMetadata(m)
		assert.NotNil(t, smtpMeta)
		assert.NotNil(t, err)
	})
}

func TestMergeWithRequestMetadata(t *testing.T) {
	t.Run("Has merged metadata", func(t *testing.T) {
		smtpMeta := Metadata{
			Host:          "mailserver.dapr.io",
			Port:          25,
			User:          "user@dapr.io",
			SkipTLSVerify: true,
			Password:      "P@$$w0rd!",
			EmailFrom:     "from@dapr.io",
			EmailTo:       "to@dapr.io",
			EmailCC:       "cc@dapr.io",
			EmailBCC:      "bcc@dapr.io",
			Subject:       "Test email",
		}

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"emailFrom": "req-from@dapr.io",
			"emailTo":   "req-to@dapr.io",
			"emailCC":   "req-cc@dapr.io",
			"emailBCC":  "req-bcc@dapr.io",
			"subject":   "req-Test email",
			"priority":  "1",
		}

		mergedMeta, err := smtpMeta.mergeWithRequestMetadata(&request)

		assert.Nil(t, err)

		assert.Equal(t, "mailserver.dapr.io", mergedMeta.Host)
		assert.Equal(t, 25, mergedMeta.Port)
		assert.Equal(t, "user@dapr.io", mergedMeta.User)
		assert.Equal(t, "P@$$w0rd!", mergedMeta.Password)
		assert.Equal(t, true, mergedMeta.SkipTLSVerify)
		assert.Equal(t, "req-from@dapr.io", mergedMeta.EmailFrom)
		assert.Equal(t, "req-to@dapr.io", mergedMeta.EmailTo)
		assert.Equal(t, "req-cc@dapr.io", mergedMeta.EmailCC)
		assert.Equal(t, "req-bcc@dapr.io", mergedMeta.EmailBCC)
		assert.Equal(t, "req-Test email", mergedMeta.Subject)
		assert.Equal(t, 1, mergedMeta.Priority)
	})
}

func TestMergeWithNoRequestMetadata(t *testing.T) {
	t.Run("Has no merged metadata", func(t *testing.T) {
		smtpMeta := Metadata{
			Host:          "mailserver.dapr.io",
			Port:          25,
			User:          "user@dapr.io",
			SkipTLSVerify: true,
			Password:      "P@$$w0rd!",
			EmailFrom:     "from@dapr.io",
			EmailTo:       "to@dapr.io",
			EmailCC:       "cc@dapr.io",
			EmailBCC:      "bcc@dapr.io",
			Subject:       "Test email",
			Priority:      1,
		}

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{}

		mergedMeta, err := smtpMeta.mergeWithRequestMetadata(&request)

		assert.Nil(t, err)
		assert.Equal(t, "mailserver.dapr.io", mergedMeta.Host)
		assert.Equal(t, 25, mergedMeta.Port)
		assert.Equal(t, "user@dapr.io", mergedMeta.User)
		assert.Equal(t, "P@$$w0rd!", mergedMeta.Password)
		assert.Equal(t, true, mergedMeta.SkipTLSVerify)
		assert.Equal(t, "from@dapr.io", mergedMeta.EmailFrom)
		assert.Equal(t, "to@dapr.io", mergedMeta.EmailTo)
		assert.Equal(t, "cc@dapr.io", mergedMeta.EmailCC)
		assert.Equal(t, "bcc@dapr.io", mergedMeta.EmailBCC)
		assert.Equal(t, "Test email", mergedMeta.Subject)
		assert.Equal(t, 1, mergedMeta.Priority)
	})
}

func TestMergeWithRequestMetadata_invalidPriorityTooHigh(t *testing.T) {
	t.Run("Has merged metadata", func(t *testing.T) {
		smtpMeta := Metadata{
			Host:          "mailserver.dapr.io",
			Port:          25,
			User:          "user@dapr.io",
			SkipTLSVerify: true,
			Password:      "P@$$w0rd!",
			EmailFrom:     "from@dapr.io",
			EmailTo:       "to@dapr.io",
			EmailCC:       "cc@dapr.io",
			EmailBCC:      "bcc@dapr.io",
			Subject:       "Test email",
			Priority:      2,
		}

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"emailFrom": "req-from@dapr.io",
			"emailTo":   "req-to@dapr.io",
			"emailCC":   "req-cc@dapr.io",
			"emailBCC":  "req-bcc@dapr.io",
			"subject":   "req-Test email",
			"priority":  "6",
		}

		mergedMeta, err := smtpMeta.mergeWithRequestMetadata(&request)

		assert.NotNil(t, mergedMeta)
		assert.NotNil(t, err)
	})
}

func TestMergeWithRequestMetadata_invalidPriorityTooLow(t *testing.T) {
	t.Run("Has merged metadata", func(t *testing.T) {
		smtpMeta := Metadata{
			Host:          "mailserver.dapr.io",
			Port:          25,
			User:          "user@dapr.io",
			SkipTLSVerify: true,
			Password:      "P@$$w0rd!",
			EmailFrom:     "from@dapr.io",
			EmailTo:       "to@dapr.io",
			EmailCC:       "cc@dapr.io",
			EmailBCC:      "bcc@dapr.io",
			Subject:       "Test email",
			Priority:      2,
		}

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"emailFrom": "req-from@dapr.io",
			"emailTo":   "req-to@dapr.io",
			"emailCC":   "req-cc@dapr.io",
			"emailBCC":  "req-bcc@dapr.io",
			"subject":   "req-Test email",
			"priority":  "0",
		}

		mergedMeta, err := smtpMeta.mergeWithRequestMetadata(&request)

		assert.NotNil(t, mergedMeta)
		assert.NotNil(t, err)
	})
}

func TestMergeWithRequestMetadata_invalidPriorityNotNumber(t *testing.T) {
	t.Run("Has merged metadata", func(t *testing.T) {
		smtpMeta := Metadata{
			Host:          "mailserver.dapr.io",
			Port:          25,
			User:          "user@dapr.io",
			SkipTLSVerify: true,
			Password:      "P@$$w0rd!",
			EmailFrom:     "from@dapr.io",
			EmailTo:       "to@dapr.io",
			EmailCC:       "cc@dapr.io",
			EmailBCC:      "bcc@dapr.io",
			Subject:       "Test email",
		}

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"emailFrom": "req-from@dapr.io",
			"emailTo":   "req-to@dapr.io",
			"emailCC":   "req-cc@dapr.io",
			"emailBCC":  "req-bcc@dapr.io",
			"subject":   "req-Test email",
			"priority":  "NoNumber",
		}

		mergedMeta, err := smtpMeta.mergeWithRequestMetadata(&request)

		assert.NotNil(t, mergedMeta)
		assert.NotNil(t, err)
	})
}
