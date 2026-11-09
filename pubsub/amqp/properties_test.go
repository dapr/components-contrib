/*
Copyright 2026 The Dapr Authors
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

package amqp

import (
	"testing"

	amqp "github.com/Azure/go-amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mdata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

func TestApplyPublishMetadata(t *testing.T) {
	tests := []struct {
		name        string
		md          map[string]string
		wantProps   map[string]any
		wantSubject string
	}{
		{
			name: "no metadata leaves the message alone",
			md:   nil,
		},
		{
			name:      "keys become string application properties",
			md:        map[string]string{"eventType": "invoice", "region": "eu"},
			wantProps: map[string]any{"eventType": "invoice", "region": "eu"},
		},
		{
			name:        "subject sets the AMQP subject, not a property",
			md:          map[string]string{"subject": "invoice"},
			wantSubject: "invoice",
		},
		{
			name: "keys that control the publish are not copied",
			md: map[string]string{
				"ttlInSeconds": "30",
				"rawPayload":   "true",
				"eventType":    "invoice",
			},
			wantProps: map[string]any{"eventType": "invoice"},
		},
		{
			name: "CloudEvent overrides are not copied",
			md: map[string]string{
				"cloudevent.type":   "com.example.invoice",
				"cloudevent.source": "billing",
				"eventType":         "invoice",
			},
			wantProps: map[string]any{"eventType": "invoice"},
		},
		{
			name: "trace context is copied",
			md: map[string]string{
				"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
			},
			wantProps: map[string]any{"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
		},
		{
			name:      "empty values are skipped",
			md:        map[string]string{"eventType": "", "subject": "", "region": "eu"},
			wantProps: map[string]any{"region": "eu"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := amqp.NewMessage([]byte("hello"))
			applyPublishMetadata(&metadata{ApplicationPropertiesFromMetadata: true}, m, tt.md)

			assert.Equal(t, tt.wantProps, m.ApplicationProperties)

			if tt.wantSubject == "" {
				if m.Properties != nil {
					assert.Nil(t, m.Properties.Subject)
				}
				return
			}

			require.NotNil(t, m.Properties)
			require.NotNil(t, m.Properties.Subject)
			assert.Equal(t, tt.wantSubject, *m.Properties.Subject)
		})
	}
}

// TestApplyPublishMetadataIsOptIn verifies that a component that does not set
// applicationPropertiesFromMetadata publishes the message unchanged.
func TestApplyPublishMetadataIsOptIn(t *testing.T) {
	m := amqp.NewMessage([]byte("hello"))
	applyPublishMetadata(&metadata{}, m, map[string]string{"eventType": "invoice", "subject": "invoice"})

	assert.Nil(t, m.ApplicationProperties)
	assert.Nil(t, m.Properties)
}

func TestParseMetadataApplicationPropertiesFromMetadata(t *testing.T) {
	log := logger.NewLogger("test")

	m, err := parseAMQPMetaData(pubsub.Metadata{Base: mdata.Base{Properties: getFakeProperties()}}, log, "", "")
	require.NoError(t, err)
	assert.False(t, m.ApplicationPropertiesFromMetadata, "off unless configured")

	props := getFakeProperties()
	props["applicationPropertiesFromMetadata"] = "true"
	m, err = parseAMQPMetaData(pubsub.Metadata{Base: mdata.Base{Properties: props}}, log, "", "")
	require.NoError(t, err)
	assert.True(t, m.ApplicationPropertiesFromMetadata)
}

// TestNewMessageAppliesPublishMetadata verifies that Publish builds its message
// with the publish metadata applied, next to the header settings.
func TestNewMessageAppliesPublishMetadata(t *testing.T) {
	a := NewAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)
	req := &pubsub.PublishRequest{
		Data:     []byte("hello"),
		Metadata: map[string]string{"eventType": "invoice", "ttlInSeconds": "30"},
	}

	m := a.newMessage(&metadata{ApplicationPropertiesFromMetadata: true, DurableMessages: true}, req, "orders")
	assert.Equal(t, map[string]any{"eventType": "invoice"}, m.ApplicationProperties)
	require.NotNil(t, m.Header)
	assert.True(t, m.Header.Durable)

	m = a.newMessage(&metadata{}, req, "orders")
	assert.Nil(t, m.ApplicationProperties, "off unless the component sets it")
}
