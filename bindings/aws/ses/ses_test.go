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

package ses

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
		m.Properties = map[string]string{
			"region":       "myRegionForSES",
			"accessKey":    "myAccessKeyForSES",
			"secretKey":    "mySecretKeyForSES",
			"sessionToken": "mySessionToken",
			"emailFrom":    "from@dapr.io",
			"emailTo":      "to@dapr.io",
			"emailCc":      "cc@dapr.io",
			"emailBcc":     "bcc@dapr.io",
			"subject":      "Test email",
		}
		r := AWSSES{logger: logger}
		smtpMeta, err := r.parseMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "myRegionForSES", smtpMeta.Region)
		assert.Equal(t, "myAccessKeyForSES", smtpMeta.AccessKey)
		assert.Equal(t, "mySecretKeyForSES", smtpMeta.SecretKey)
		assert.Equal(t, "mySessionToken", smtpMeta.SessionToken)
		assert.Equal(t, "from@dapr.io", smtpMeta.EmailFrom)
		assert.Equal(t, "to@dapr.io", smtpMeta.EmailTo)
		assert.Equal(t, "cc@dapr.io", smtpMeta.EmailCc)
		assert.Equal(t, "bcc@dapr.io", smtpMeta.EmailBcc)
		assert.Equal(t, "Test email", smtpMeta.Subject)
	})

	t.Run("region is required", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"accessKey": "myAccessKeyForSES",
			"secretKey": "mySecretKeyForSES",
			"emailFrom": "from@dapr.io",
			"emailTo":   "to@dapr.io",
			"emailCc":   "cc@dapr.io",
			"emailBcc":  "bcc@dapr.io",
			"subject":   "Test email",
		}
		r := AWSSES{logger: logger}
		_, err := r.parseMetadata(m)
		assert.Error(t, err)
	})

	t.Run("accessKey is required", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"region":    "myRegionForSES",
			"secretKey": "mySecretKeyForSES",
			"emailFrom": "from@dapr.io",
			"emailTo":   "to@dapr.io",
			"emailCc":   "cc@dapr.io",
			"emailBcc":  "bcc@dapr.io",
			"subject":   "Test email",
		}
		r := AWSSES{logger: logger}
		_, err := r.parseMetadata(m)
		assert.Error(t, err)
	})

	t.Run("secretKey is required", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"region":    "myRegionForSES",
			"accessKey": "myAccessKeyForSES",
			"emailFrom": "from@dapr.io",
			"emailTo":   "to@dapr.io",
			"emailCc":   "cc@dapr.io",
			"emailBcc":  "bcc@dapr.io",
			"subject":   "Test email",
		}
		r := AWSSES{logger: logger}
		_, err := r.parseMetadata(m)
		assert.Error(t, err)
	})
}

func TestMergeWithRequestMetadata(t *testing.T) {
	t.Run("Has merged metadata", func(t *testing.T) {
		sesMeta := sesMetadata{
			Region:    "myRegionForSES",
			AccessKey: "myAccessKeyForSES",
			SecretKey: "mySecretKeyForSES",
			EmailFrom: "from@dapr.io",
			EmailTo:   "to@dapr.io",
			EmailCc:   "cc@dapr.io",
			EmailBcc:  "bcc@dapr.io",
			Subject:   "Test email",
		}

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"emailFrom": "req-from@dapr.io",
			"emailTo":   "req-to@dapr.io",
			"emailCc":   "req-cc@dapr.io",
			"emailBcc":  "req-bcc@dapr.io",
			"subject":   "req-Test email",
		}

		mergedMeta := sesMeta.mergeWithRequestMetadata(&request)

		assert.Equal(t, "myRegionForSES", mergedMeta.Region)
		assert.Equal(t, "myAccessKeyForSES", mergedMeta.AccessKey)
		assert.Equal(t, "mySecretKeyForSES", mergedMeta.SecretKey)
		assert.Equal(t, "req-from@dapr.io", mergedMeta.EmailFrom)
		assert.Equal(t, "req-to@dapr.io", mergedMeta.EmailTo)
		assert.Equal(t, "req-cc@dapr.io", mergedMeta.EmailCc)
		assert.Equal(t, "req-bcc@dapr.io", mergedMeta.EmailBcc)
		assert.Equal(t, "req-Test email", mergedMeta.Subject)
	})

	t.Run("Has no merged metadata", func(t *testing.T) {
		sesMeta := sesMetadata{
			Region:    "myRegionForSES",
			AccessKey: "myAccessKeyForSES",
			SecretKey: "mySecretKeyForSES",
			EmailFrom: "from@dapr.io",
			EmailTo:   "to@dapr.io",
			EmailCc:   "cc@dapr.io",
			EmailBcc:  "bcc@dapr.io",
			Subject:   "Test email",
		}

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{}

		mergedMeta := sesMeta.mergeWithRequestMetadata(&request)

		assert.Equal(t, "myRegionForSES", mergedMeta.Region)
		assert.Equal(t, "myAccessKeyForSES", mergedMeta.AccessKey)
		assert.Equal(t, "mySecretKeyForSES", mergedMeta.SecretKey)
		assert.Equal(t, "from@dapr.io", mergedMeta.EmailFrom)
		assert.Equal(t, "to@dapr.io", mergedMeta.EmailTo)
		assert.Equal(t, "cc@dapr.io", mergedMeta.EmailCc)
		assert.Equal(t, "bcc@dapr.io", mergedMeta.EmailBcc)
		assert.Equal(t, "Test email", mergedMeta.Subject)
	})
}
