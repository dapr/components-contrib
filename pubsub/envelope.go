// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

import (
	"time"

	contrib_metadata "github.com/dapr/components-contrib/metadata"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
)

const (
	// DefaultCloudEventType is the default event type for an Dapr published event
	DefaultCloudEventType = "com.dapr.event.sent"
	// CloudEventsSpecVersion is the specversion used by Dapr for the cloud events implementation
	CloudEventsSpecVersion = "1.0"
	// ContentType is the Cloud Events HTTP content type
	ContentType = "application/cloudevents+json"
	// DefaultCloudEventSource is the default event source
	DefaultCloudEventSource = "Dapr"
	// DefaultCloudEventDataContentType is the default content-type for the data attribute
	DefaultCloudEventDataContentType = "text/plain"
	DaprTraceIDField                 = "daprtraceid"
)

// CloudEventsEnvelope describes the Dapr implementation of the Cloud Events spec
// Spec details: https://github.com/cloudevents/spec/blob/master/spec.md
type CloudEventsEnvelope struct {
	ID              string      `json:"id"`
	Source          string      `json:"source"`
	Type            string      `json:"type"`
	SpecVersion     string      `json:"specversion"`
	DataContentType string      `json:"datacontenttype"`
	Data            interface{} `json:"data"`
	Subject         string      `json:"subject"`
	Topic           string      `json:"topic"`
	PubsubName      string      `json:"pubsubname"`
	Expiration      string      `json:"expiration,omitempty"`
	DaprTraceID     string      `json:"daprtraceid"`
}

// NewCloudEventsEnvelope returns CloudEventsEnvelope from data or a new one when data content was not
func NewCloudEventsEnvelope(id, source, eventType, subject string, topic string, pubsubName string, dataContentType string, data []byte, traceID string) *CloudEventsEnvelope {
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

	// check if JSON
	var j interface{}
	err := jsoniter.Unmarshal(data, &j)
	if err != nil {
		// not JSON, return new envelope
		return &CloudEventsEnvelope{
			ID:              id,
			SpecVersion:     CloudEventsSpecVersion,
			DataContentType: dataContentType,
			Source:          source,
			Type:            eventType,
			Subject:         subject,
			Topic:           topic,
			PubsubName:      pubsubName,
			Data:            string(data),
			DaprTraceID:     traceID,
		}
	}

	// content was JSON but not a valid CloudEvent, make one
	return &CloudEventsEnvelope{
		ID:              id,
		SpecVersion:     CloudEventsSpecVersion,
		DataContentType: "application/json",
		Source:          source,
		Type:            eventType,
		Subject:         subject,
		Topic:           topic,
		PubsubName:      pubsubName,
		Data:            j,
		DaprTraceID:     traceID,
	}
}

// SetTraceID adds the daprtraceid field to an existing cloudevents JSON
func SetTraceID(data []byte, traceID string) ([]byte, error) {
	var m map[string]interface{}

	err := jsoniter.Unmarshal(data, &m)
	if err != nil {
		return data, err
	}

	m[DaprTraceIDField] = traceID
	b, err := jsoniter.Marshal(m)
	if err != nil {
		return data, err
	}

	return b, nil
}

// HasExpired determines if the current cloud event has expired.
func (cloudEvent *CloudEventsEnvelope) HasExpired() bool {
	if cloudEvent.Expiration != "" {
		expiration, err := time.Parse(time.RFC3339, cloudEvent.Expiration)
		if err != nil {
			return false
		}

		return expiration.UTC().Before(time.Now().UTC())
	}

	return false
}

// ApplyMetadata will process metadata to modify the cloud event based on the component's feature set.
func (cloudEvent *CloudEventsEnvelope) ApplyMetadata(componentFeatures []Feature, metadata map[string]string) {
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
		cloudEvent.Expiration = expiration.Format(time.RFC3339)
	}
}
