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
	"strings"

	amqp "github.com/Azure/go-amqp"

	contribMetadata "github.com/dapr/components-contrib/metadata"
)

const (
	// subjectMetadataKey sets the AMQP subject of a message. ActiveMQ Artemis
	// exposes the subject to selectors as JMSType.
	subjectMetadataKey = "subject"

	// ttlInSecondsMetadataKey sets the TTL of a message, see messageTTL.
	ttlInSecondsMetadataKey = "ttlInSeconds"

	// cloudEventMetadataPrefix marks the keys that override fields of the
	// CloudEvent envelope, for example cloudevent.type.
	cloudEventMetadataPrefix = "cloudevent."
)

// applyPublishMetadata copies the metadata of a publish request onto an AMQP
// message, if the component sets applicationPropertiesFromMetadata. The subject
// key sets the subject. Every other key becomes an application property with a
// string value, which is what a broker-side filter such as an ActiveMQ Artemis
// selector matches on.
//
// Keys that control the publish itself are not copied, and neither are
// cloudevent.* keys: they override fields of the CloudEvent envelope, which is
// in the message body already. Empty values are skipped.
func applyPublishMetadata(md *metadata, m *amqp.Message, reqMetadata map[string]string) {
	if !md.ApplicationPropertiesFromMetadata {
		return
	}

	for key, value := range reqMetadata {
		if value == "" || !copiedToMessage(key) {
			continue
		}

		if key == subjectMetadataKey {
			if m.Properties == nil {
				m.Properties = &amqp.MessageProperties{}
			}
			subject := value
			m.Properties.Subject = &subject

			continue
		}

		if m.ApplicationProperties == nil {
			m.ApplicationProperties = make(map[string]any, len(reqMetadata))
		}
		m.ApplicationProperties[key] = value
	}
}

// copiedToMessage reports whether a publish metadata key is copied onto the
// message.
func copiedToMessage(key string) bool {
	switch key {
	case ttlInSecondsMetadataKey, contribMetadata.RawPayloadKey:
		return false
	}

	return !strings.HasPrefix(key, cloudEventMetadataPrefix)
}
