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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	"github.com/sendgrid/sendgrid-go/helpers/mail"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInit(t *testing.T) {
	logger := logger.NewLogger("test")

	t.Run("apiKey is required", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{}

		testBinding := NewSendGrid(logger).(*SendGrid)
		err := testBinding.Init(context.Background(), m)
		require.Error(t, err, "apiKey field is required in metadata")
	})

	t.Run("invalid metadata", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"asmGroupId": "not an integer"}

		testBinding := NewSendGrid(logger).(*SendGrid)
		err := testBinding.Init(context.Background(), m)
		require.ErrorContains(t, err, "cannot parse 'asmGroupId'")
	})

	t.Run("Has correct metadata", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"apiKey":    "123",
			"emailFrom": "test1@example.net",
			"emailTo":   "test2@example.net",
			"subject":   "hello",
		}
		testBinding := NewSendGrid(logger).(*SendGrid)
		err := testBinding.Init(context.Background(), m)
		require.NoError(t, err)

		sgMeta := testBinding.metadata
		assert.Equal(t, "123", sgMeta.APIKey)
		assert.Equal(t, "test1@example.net", sgMeta.EmailFrom)
		assert.Equal(t, "test2@example.net", sgMeta.EmailTo)
		assert.Equal(t, "hello", sgMeta.Subject)
		assert.Equal(t, "", sgMeta.DynamicTemplateData)
		assert.Equal(t, "", sgMeta.DynamicTemplateID)
		assert.Nil(t, sgMeta.AsmGroupID)
		assert.Equal(t, "", sgMeta.AsmGroupsToDisplay)
	})

	t.Run("Has correct metadata with optional names", func(t *testing.T) {
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
			"asmGroupId":          "789",
			"asmGroupsToDisplay":  "1,2,3",
		}

		testBinding := NewSendGrid(logger).(*SendGrid)
		err := testBinding.Init(context.Background(), m)
		require.NoError(t, err)

		sgMeta := testBinding.metadata
		assert.Equal(t, "123", sgMeta.APIKey)
		assert.Equal(t, "test1@example.net", sgMeta.EmailFrom)
		assert.Equal(t, "test 1", sgMeta.EmailFromName)
		assert.Equal(t, "test2@example.net", sgMeta.EmailTo)
		assert.Equal(t, "test 2", sgMeta.EmailToName)
		assert.Equal(t, "hello", sgMeta.Subject)
		assert.Equal(t, `{"name":{"first":"MyFirst","last":"MyLast"}}`, sgMeta.DynamicTemplateData)
		assert.Equal(t, "456", sgMeta.DynamicTemplateID)
		assert.Equal(t, 789, *sgMeta.AsmGroupID)
		assert.Equal(t, "1,2,3", sgMeta.AsmGroupsToDisplay)
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

		testBinding := NewSendGrid(logger).(*SendGrid)
		err := testBinding.Init(context.Background(), m)
		require.ErrorContains(t, err, "error from SendGrid binding, dynamic template data is not valid JSON")
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
		require.NoError(t, err)
		assert.Equal(t, map[string]interface{}{"first": "MyFirst", "last": "MyLast"}, data["name"])

		// Test invalid JSON
		err = UnmarshalDynamicTemplateData("{\"wrong\"}", &data)
		require.Error(t, err)
	})
}

func TestInvoke(t *testing.T) {
	testLogger := logger.NewLogger("test")

	bindingMetadataProperties := map[string]string{
		"apiKey":              "apiKey",
		"emailFrom":           "from@mail.com",
		"emailTo":             "to@email.com",
		"subject":             "subject",
		"emailCc":             "cc@mail.com",
		"emailBcc":            "bcc@mail.com",
		"emailFromName":       "from name",
		"emailToName":         "to name",
		"dynamicTemplateData": "{\"name\":\"value\"}",
		"dynamicTemplateId":   "template id",
		"asmGroupId":          "123",
		"asmGroupsToDisplay":  "1,2,3",
	}
	overrideMetadataProperties := map[string]string{
		"emailFrom":           "override-from@mail.com",
		"emailTo":             "override-to@email.com",
		"subject":             "override subject",
		"emailCc":             "override-cc@mail.com",
		"emailBcc":            "override-bcc@mail.com",
		"emailFromName":       "override from name",
		"emailToName":         "override to name",
		"dynamicTemplateData": "{\"hello\":\"override\"}",
		"dynamicTemplateId":   "override template id",
		"asmGroupId":          "12345",
		"asmGroupsToDisplay":  "4,5,6,7,9",
	}

	successRequest := &bindings.InvokeRequest{
		Operation: bindings.CreateOperation,
	}

	t.Run("non-200 sendgrid response", func(t *testing.T) {
		testBinding := makeTestBinding(t, testLogger, bindingMetadataProperties)
		testServer := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		})

		testBinding.baseURL = testServer.URL

		_, err := testBinding.Invoke(context.Background(), successRequest)
		require.ErrorContains(t, err, "error from SendGrid: sending email failed: 500")
	})

	t.Run("sends API key as authorization header", func(t *testing.T) {
		testBinding := makeTestBinding(t, testLogger, bindingMetadataProperties)
		testServer := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
			assert.Contains(t, r.Header, "Authorization")
			assert.Equal(t, "Bearer apiKey", r.Header.Get("Authorization"))

			writeSuccessResponse(w)
		})
		testBinding.baseURL = testServer.URL

		_, _ = testBinding.Invoke(context.Background(), successRequest)
	})

	t.Run("uses binding metadata defaults", func(t *testing.T) {
		testBinding := makeTestBinding(t, testLogger, bindingMetadataProperties)
		testServer := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
			var email mail.SGMailV3
			err := json.NewDecoder(r.Body).Decode(&email)
			require.NoError(t, err)
			assert.Equal(t, email.From.Name, bindingMetadataProperties["emailFromName"])
			assert.Equal(t, email.From.Address, bindingMetadataProperties["emailFrom"])
			assert.Equal(t, email.Personalizations[0].To[0].Name, bindingMetadataProperties["emailToName"])
			assert.Equal(t, email.Personalizations[0].To[0].Address, bindingMetadataProperties["emailTo"])
			assert.Equal(t, email.Personalizations[0].Subject, bindingMetadataProperties["subject"])
			assert.Equal(t, email.Personalizations[0].CC[0].Address, bindingMetadataProperties["emailCc"])
			assert.Equal(t, email.Personalizations[0].BCC[0].Address, bindingMetadataProperties["emailBcc"])
			assert.Equal(t, email.TemplateID, bindingMetadataProperties["dynamicTemplateId"])
			assert.Equal(t, email.Personalizations[0].DynamicTemplateData, map[string]any{"name": "value"})
			assert.Equal(t, email.Asm.GroupID, 123)
			assert.Equal(t, email.Asm.GroupsToDisplay, []int{1, 2, 3})

			writeSuccessResponse(w)
		})
		testBinding.baseURL = testServer.URL

		_, _ = testBinding.Invoke(context.Background(), successRequest)
	})

	t.Run("uses request metadata overrides", func(t *testing.T) {
		testBinding := makeTestBinding(t, testLogger, bindingMetadataProperties)
		testServer := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
			var email mail.SGMailV3
			err := json.NewDecoder(r.Body).Decode(&email)
			require.NoError(t, err)
			assert.Equal(t, email.From.Name, overrideMetadataProperties["emailFromName"])
			assert.Equal(t, email.From.Address, overrideMetadataProperties["emailFrom"])
			assert.Equal(t, email.Personalizations[0].To[0].Name, overrideMetadataProperties["emailToName"])
			assert.Equal(t, email.Personalizations[0].To[0].Address, overrideMetadataProperties["emailTo"])
			assert.Equal(t, email.Personalizations[0].Subject, overrideMetadataProperties["subject"])
			assert.Equal(t, email.Personalizations[0].CC[0].Address, overrideMetadataProperties["emailCc"])
			assert.Equal(t, email.Personalizations[0].BCC[0].Address, overrideMetadataProperties["emailBcc"])
			assert.Equal(t, email.TemplateID, overrideMetadataProperties["dynamicTemplateId"])
			assert.Equal(t, email.Personalizations[0].DynamicTemplateData, map[string]any{"hello": "override"})
			assert.Equal(t, email.Asm.GroupID, 12345)
			assert.Equal(t, email.Asm.GroupsToDisplay, []int{4, 5, 6, 7, 9})

			writeSuccessResponse(w)
		})
		testBinding.baseURL = testServer.URL

		overrideMetadataRequest := &bindings.InvokeRequest{
			Operation: bindings.CreateOperation,
			Metadata:  overrideMetadataProperties,
		}

		_, _ = testBinding.Invoke(context.Background(), overrideMetadataRequest)
	})

	t.Run("ASM not set if asmGroupId is not provided", func(t *testing.T) {
		newBindingMetadataProperties := copyMap(bindingMetadataProperties)
		delete(newBindingMetadataProperties, "asmGroupId")
		testBinding := makeTestBinding(t, testLogger, newBindingMetadataProperties)
		testServer := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
			var email mail.SGMailV3
			err := json.NewDecoder(r.Body).Decode(&email)
			require.NoError(t, err)
			assert.Nil(t, email.Asm)

			writeSuccessResponse(w)
		})
		testBinding.baseURL = testServer.URL

		_, _ = testBinding.Invoke(context.Background(), successRequest)
	})

	t.Run("email from is required", func(t *testing.T) {
		newBindingMetadataProperties := copyMap(bindingMetadataProperties)
		newBindingMetadataProperties["emailFrom"] = ""
		testBinding := makeTestBinding(t, testLogger, newBindingMetadataProperties)

		overrideMetadataRequest := &bindings.InvokeRequest{
			Operation: bindings.CreateOperation,
			Metadata:  map[string]string{},
		}

		_, err := testBinding.Invoke(context.Background(), overrideMetadataRequest)
		require.Error(t, err, "error SendGrid from email not supplied")

	})

	t.Run("subject is required", func(t *testing.T) {
		newBindingMetadataProperties := copyMap(bindingMetadataProperties)
		newBindingMetadataProperties["subject"] = ""
		testBinding := makeTestBinding(t, testLogger, newBindingMetadataProperties)

		overrideMetadataRequest := &bindings.InvokeRequest{
			Operation: bindings.CreateOperation,
			Metadata:  map[string]string{},
		}

		_, err := testBinding.Invoke(context.Background(), overrideMetadataRequest)
		require.Error(t, err, "error SendGrid subject not supplied")
	})

	t.Run("email to is required", func(t *testing.T) {
		newBindingMetadataProperties := copyMap(bindingMetadataProperties)
		newBindingMetadataProperties["emailTo"] = ""
		testBinding := makeTestBinding(t, testLogger, newBindingMetadataProperties)

		overrideMetadataRequest := &bindings.InvokeRequest{
			Operation: bindings.CreateOperation,
			Metadata:  map[string]string{},
		}

		_, err := testBinding.Invoke(context.Background(), overrideMetadataRequest)
		require.Error(t, err, "error SendGrid to email not supplied")

	})

	t.Run("invalid asm group id", func(t *testing.T) {
		newBindingMetadataProperties := copyMap(bindingMetadataProperties)
		delete(newBindingMetadataProperties, "asmGroupId")
		testBinding := makeTestBinding(t, testLogger, newBindingMetadataProperties)

		overrideMetadataRequest := &bindings.InvokeRequest{
			Operation: bindings.CreateOperation,
			Metadata:  map[string]string{"asmGroupId": "not an integer"},
		}

		_, err := testBinding.Invoke(context.Background(), overrideMetadataRequest)
		require.ErrorContains(t, err, "error SendGrid asmGroupId is not a valid integer:")
	})

	t.Run("invalid asm groups to display", func(t *testing.T) {
		newBindingMetadataProperties := copyMap(bindingMetadataProperties)
		delete(newBindingMetadataProperties, "asmGroupsToDisplay")
		testBinding := makeTestBinding(t, testLogger, newBindingMetadataProperties)

		overrideMetadataRequest := &bindings.InvokeRequest{
			Operation: bindings.CreateOperation,
			Metadata:  map[string]string{"asmGroupsToDisplay": "error"},
		}

		_, err := testBinding.Invoke(context.Background(), overrideMetadataRequest)
		require.ErrorContains(t, err, "error SendGrid asmGroupsToDisplay is not a valid integer:")
	})
}

func TestOperations(t *testing.T) {
	testLogger := logger.NewLogger("test")
	testBinding := NewSendGrid(testLogger).(*SendGrid)
	operations := testBinding.Operations()
	assert.Len(t, operations, 1)
	assert.Equal(t, bindings.CreateOperation, operations[0])
}

func copyMap(m map[string]string) map[string]string {
	m2 := make(map[string]string)
	for k, v := range m {
		m2[k] = v
	}
	return m2
}

func makeTestBinding(t *testing.T, logger logger.Logger, bindingMetadataProperties map[string]string) *SendGrid {
	binding := NewSendGrid(logger).(*SendGrid)
	bindingMetadata := bindings.Metadata{
		Base: metadata.Base{
			Properties: bindingMetadataProperties,
		},
	}
	err := binding.Init(context.Background(), bindingMetadata)
	require.NoError(t, err)
	return binding
}

func writeSuccessResponse(w http.ResponseWriter) {
	w.WriteHeader(http.StatusOK)
}

type handleFunc func(http.ResponseWriter, *http.Request)

func newTestServer(t *testing.T, fn handleFunc) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(fn))
	return server
}
