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

package pubsub

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	contrib_contenttype "github.com/dapr/components-contrib/contenttype"
	contrib_metadata "github.com/dapr/components-contrib/metadata"
)

const (
	// DefaultCloudEventType is the default event type for an Dapr published event.
	DefaultCloudEventType = "com.dapr.event.sent"
	// CloudEventsSpecVersion is the specversion used by Dapr for the cloud events implementation.
	CloudEventsSpecVersion = "1.0"
	// DefaultCloudEventSource is the default event source.
	DefaultCloudEventSource = "Dapr"
	// DefaultCloudEventDataContentType is the default content-type for the data attribute.
	DefaultCloudEventDataContentType = "text/plain"
	// traceid, backwards compatibles.
	// ::TODO delete traceid, and keep traceparent.
	TraceIDField         = "traceid"
	TraceParentField     = "traceparent"
	TraceStateField      = "tracestate"
	TopicField           = "topic"
	PubsubField          = "pubsubname"
	ExpirationField      = "expiration"
	DataContentTypeField = "datacontenttype"
	DataField            = "data"
	DataBase64Field      = "data_base64"
	SpecVersionField     = "specversion"
	TypeField            = "type"
	SourceField          = "source"
	IDField              = "id"
	SubjectField         = "subject"
)

var CloudEventMetadata = [...]string{TraceIDField, TraceParentField, TraceStateField, TopicField, PubsubField, ExpirationField, DataContentTypeField, DataField, DataBase64Field, SpecVersionField, TypeField, SourceField, IDField, SubjectField}

// IsCloudEventMetadata Checks if key is a valid cloudevent metadata
func IsCloudEventMetadata(k string) bool {
	for _, field := range CloudEventMetadata {
		if field == k {
			return true
		}
	}
	return false
}

// unmarshalPrecise is a wrapper around encoding/json's Decoder
// with UseNumber. It prevents data loss for big numbers
// while unmarshalling.
func unmarshalPrecise(data []byte, v interface{}) error {
	decoder := json.NewDecoder(bytes.NewBuffer(data))
	decoder.UseNumber()
	if err := decoder.Decode(v); err != nil {
		return err
	}
	return nil
}

// NewCloudEventsEnvelope returns a map representation of a cloudevents JSON.
func NewCloudEventsEnvelope(id, source, eventType, subject string, topic string, pubsubName string,
	dataContentType string, data []byte, traceParent string, traceState string,
) map[string]interface{} {
	var ceData interface{}
	ceDataField := DataField
	var err error
	if contrib_contenttype.IsJSONContentType(dataContentType) {
		err = unmarshalPrecise(data, &ceData)
	} else if contrib_contenttype.IsBinaryContentType(dataContentType) {
		ceData = base64.StdEncoding.EncodeToString(data)
		ceDataField = DataBase64Field
	} else {
		ceData = string(data)
	}

	if err != nil {
		ceData = string(data)
	}

	ce := NewCloudEventMetadata(id, source, eventType, subject, topic, pubsubName, dataContentType, traceParent, traceState)

	ce[ceDataField] = ceData

	return ce
}

// NewCloudEventsHeaders returns a map representation of a cloudevents headers.
func NewCloudEventMetadata(id, source, eventType, subject string, topic string, pubsubName string,
	dataContentType string, traceParent string, traceState string,
) map[string]interface{} {
	// defaults
	if id == "" {
		id = uuid.New().String()
	}
	if source == "" {
		source = DefaultCloudEventSource
	}
	if eventType == "" {
		eventType = DefaultCloudEventType
	}
	if dataContentType == "" {
		dataContentType = DefaultCloudEventDataContentType
	}

	ce := map[string]interface{}{
		IDField:              id,
		SpecVersionField:     CloudEventsSpecVersion,
		DataContentTypeField: dataContentType,
		SourceField:          source,
		TypeField:            eventType,
		TopicField:           topic,
		PubsubField:          pubsubName,
		TraceIDField:         traceParent,
		TraceParentField:     traceParent,
		TraceStateField:      traceState,
	}

	if subject != "" {
		ce[SubjectField] = subject
	}

	return ce
}

// FromCloudEvent returns a map representation of an existing cloudevents JSON.
func FromCloudEvent(cloudEvent []byte, topic, pubsub, traceParent string, traceState string) (map[string]interface{}, error) {
	var m map[string]interface{}
	err := unmarshalPrecise(cloudEvent, &m)
	if err != nil {
		return m, err
	}
	UpdateCloudEventMetadata(m, topic, pubsub, traceParent, traceParent, traceState, DefaultCloudEventDataContentType)

	return m, nil
}

func UpdateCloudEventMetadata(metadata map[string]interface{}, topic string, pubsub string, traceId string, traceParent string, traceState string, contentType string) {

	metadata[TraceIDField] = traceId
	metadata[TraceParentField] = traceParent
	metadata[TraceStateField] = traceState
	metadata[TopicField] = topic
	metadata[PubsubField] = pubsub
	metadata[DataContentTypeField] = contentType

	// default values
	if metadata[SourceField] == nil {
		metadata[SourceField] = DefaultCloudEventSource
	}

	if metadata[TypeField] == nil {
		metadata[TypeField] = DefaultCloudEventType
	}

	if metadata[SpecVersionField] == nil {
		metadata[SpecVersionField] = CloudEventsSpecVersion
	}
}

// FromRawPayload returns a CloudEvent for a raw payload on subscriber's end.
func FromRawPayload(data []byte, topic, pubsub string) map[string]interface{} {
	// Limitations of generating the CloudEvent on the subscriber side based on raw payload:
	// - The CloudEvent ID will be random, so the same message can be redelivered as a different ID.
	// - TraceID is not useful since it is random and not from publisher side.
	// - Data is always returned as `data_base64` since we don't know the actual content type.
	return map[string]interface{}{
		IDField:              uuid.New().String(),
		SpecVersionField:     CloudEventsSpecVersion,
		DataContentTypeField: "application/octet-stream",
		SourceField:          DefaultCloudEventSource,
		TypeField:            DefaultCloudEventType,
		TopicField:           topic,
		PubsubField:          pubsub,
		DataBase64Field:      base64.StdEncoding.EncodeToString(data),
	}
}

// FromBinaryModePayload returns a CloudEvent for a binary mode payload on subscriber's end.
func FromBinaryModePayload(data []byte, metadata map[string]string, topic, pubsub string) map[string]interface{} {
	m := make(map[string]interface{})
	m = FromRawPayload(data, topic, pubsub)
	for k, v := range metadata {
		if IsCloudEventMetadata(k) {
			m[k] = v
		}
	}
	return m
}

// HasExpired determines if the current cloud event has expired.
func HasExpired(cloudEvent map[string]interface{}) bool {
	e, ok := cloudEvent[ExpirationField]
	if ok && e != "" {
		expiration, err := time.Parse(time.RFC3339, fmt.Sprintf("%s", e))
		if err != nil {
			return false
		}

		return expiration.UTC().Before(time.Now().UTC())
	}

	return false
}

// ApplyMetadata will process metadata to modify the cloud event based on the component's feature set.
func ApplyMetadata(cloudEvent map[string]interface{}, componentFeatures []Feature, metadata map[string]string) {
	ttl, hasTTL, _ := contrib_metadata.TryGetTTL(metadata)
	if hasTTL && !FeatureMessageTTL.IsPresent(componentFeatures) {
		// Dapr only handles Message TTL if component does not.
		now := time.Now().UTC()
		// The maximum ttl is maxInt64, which is not enough to overflow time, for now.
		// As of the time this code was written (2020 Dec 28th),
		// the maximum time of now() adding maxInt64 is ~ "2313-04-09T23:30:26Z".
		// Max time in golang is currently 292277024627-12-06T15:30:07.999999999Z.
		// So, we have some time before the overflow below happens :)
		expiration := now.Add(ttl)
		cloudEvent[ExpirationField] = expiration.Format(time.RFC3339)
	}
}
