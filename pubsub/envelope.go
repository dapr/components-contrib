// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

import (
	"encoding/base64"
	"fmt"
	"time"

	contrib_contenttype "github.com/dapr/components-contrib/contenttype"
	contrib_metadata "github.com/dapr/components-contrib/metadata"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
)

const (
	// DefaultCloudEventType is the default event type for an Dapr published event
	DefaultCloudEventType = "com.dapr.event.sent"
	// CloudEventsSpecVersion is the specversion used by Dapr for the cloud events implementation
	CloudEventsSpecVersion = "1.0"
	// DefaultCloudEventSource is the default event source
	DefaultCloudEventSource = "Dapr"
	// DefaultCloudEventDataContentType is the default content-type for the data attribute
	DefaultCloudEventDataContentType = "text/plain"
	TraceIDField                     = "traceid"
	TopicField                       = "topic"
	PubsubField                      = "pubsubname"
	ExpirationField                  = "expiration"
	DataContentTypeField             = "datacontenttype"
	DataField                        = "data"
	DataBase64Field                  = "data_base64"
	SpecVersionField                 = "specversion"
	TypeField                        = "type"
	SourceField                      = "source"
	IDField                          = "id"
	SubjectField                     = "subject"
)

// NewCloudEventsEnvelope returns a map representation of a cloudevents JSON
func NewCloudEventsEnvelope(id, source, eventType, subject string, topic string, pubsubName string, dataContentType string, data []byte, traceID string) map[string]interface{} {
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

	var ceData interface{}
	ceDataField := DataField
	var err error
	if contrib_contenttype.IsJSONContentType(dataContentType) {
		err = jsoniter.Unmarshal(data, &ceData)
	} else if contrib_contenttype.IsBinaryContentType(dataContentType) {
		ceData = base64.StdEncoding.EncodeToString(data)
		ceDataField = DataBase64Field
	} else {
		ceData = string(data)
	}

	if err != nil {
		ceData = string(data)
	}

	ce := map[string]interface{}{
		IDField:              id,
		SpecVersionField:     CloudEventsSpecVersion,
		DataContentTypeField: dataContentType,
		SourceField:          source,
		TypeField:            eventType,
		TopicField:           topic,
		PubsubField:          pubsubName,
		TraceIDField:         traceID,
	}

	ce[ceDataField] = ceData

	if subject != "" {
		ce[SubjectField] = subject
	}

	return ce
}

// FromCloudEvent returns a map representation of an existing cloudevents JSON
func FromCloudEvent(cloudEvent []byte, topic, pubsub, traceID string) (map[string]interface{}, error) {
	var m map[string]interface{}
	err := jsoniter.Unmarshal(cloudEvent, &m)
	if err != nil {
		return m, err
	}

	m[TraceIDField] = traceID
	m[TopicField] = topic
	m[PubsubField] = pubsub

	return m, nil
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
