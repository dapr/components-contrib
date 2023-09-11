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

package sendgrid

import (
	"encoding/json"
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

func TestParseMetadataWithOptionalNames(t *testing.T) {
	logger := logger.NewLogger("test")

	t.Run("Has correct metadata", func(t *testing.T) {
		// Sample nested JSON with Dynamic Template Data
		dynamicTemplateData, _ := json.Marshal(map[string]any{
			"name": map[string]any{
				"first": "MyFirst",
				"last":  "MyLast",
			},
		})

		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"apiKey":        "123",
			"emailFrom":     "test1@example.net",
			"emailFromName": "test 1",
			"emailTo":       "test2@example.net",
			"emailToName":   "test 2", "subject": "hello",
			"dynamicTemplateData": string(dynamicTemplateData),
			"dynamicTemplateId":   "456",
		}
		r := SendGrid{logger: logger}
		sgMeta, err := r.parseMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "123", sgMeta.APIKey)
		assert.Equal(t, "test1@example.net", sgMeta.EmailFrom)
		assert.Equal(t, "test 1", sgMeta.EmailFromName)
		assert.Equal(t, "test2@example.net", sgMeta.EmailTo)
		assert.Equal(t, "test 2", sgMeta.EmailToName)
		assert.Equal(t, "hello", sgMeta.Subject)
		assert.Equal(t, `{"name":{"first":"MyFirst","last":"MyLast"}}`, sgMeta.DynamicTemplateData)
		assert.Equal(t, "456", sgMeta.DynamicTemplateID)
	})

	t.Run("Has incorrect template data metadata", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"apiKey":              "123",
			"emailFrom":           "test1@example.net",
			"emailFromName":       "test 1",
			"emailTo":             "test2@example.net",
			"emailToName":         "test 2",
			"subject":             "hello",
			"dynamicTemplateData": `{"wrong"}`,
			"dynamicTemplateId":   "456",
		}
		r := SendGrid{logger: logger}
		_, err := r.parseMetadata(m)
		assert.Error(t, err)
	})
}

// Test UnmarshalDynamicTemplateData function
func TestUnmarshalDynamicTemplateData(t *testing.T) {
	t.Run("Test Template Data", func(t *testing.T) {
		// Sample nested JSON with Dynamic Template Data
		dynamicTemplateData, _ := json.Marshal(map[string]interface{}{"name": map[string]interface{}{"first": "MyFirst", "last": "MyLast"}})

		var data map[string]interface{}

		// Test valid JSON
		err := UnmarshalDynamicTemplateData(string(dynamicTemplateData), &data)
		assert.NoError(t, err)
		assert.Equal(t, map[string]interface{}{"first": "MyFirst", "last": "MyLast"}, data["name"])

		// Test invalid JSON
		err = UnmarshalDynamicTemplateData("{\"wrong\"}", &data)
		assert.Error(t, err)
	})
}
