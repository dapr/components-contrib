// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

import (
	"github.com/google/uuid"
)

const (
	// DefaultCloudEventType is the default event type for an Dapr published event
	DefaultCloudEventType = "com.dapr.event.sent"
	// CloudEventsSpecVersion is the specversion used by Dapr for the cloud events implementation
	CloudEventsSpecVersion = "1.0"
	// DefaultContentType is the default Cloud Events Envelope content type.
	DefaultContentType = "application/json"
	// DefaultCloudEventSource is the default event source
	DefaultCloudEventSource = "Dapr"
)

// CloudEventsEnvelope describes the Dapr implementation of the Cloud Events spec
// Spec details: https://github.com/cloudevents/spec/blob/master/spec.md
type CloudEventsEnvelope struct {
	ID              string `json:"id"`
	Source          string `json:"source"`
	Type            string `json:"type"`
	SpecVersion     string `json:"specversion"`
	DataContentType string `json:"datacontenttype"`
	Data            []byte `json:"data"`
	Subject         string `json:"subject"`
	Topic           string `json:"topic"`
	PubsubName      string `json:"pubsubname"`
}

// NewDaprEventWithRequest new a event which support the CloudEvent specversion 1.0 from PublishRequest.
func NewDaprCloudEventWithRequest(req *PublishRequest) *CloudEventsEnvelope {
	return NewCloudEventsEnvelope("", "", "", "", "", req.Topic, req.PubsubName, req.Data)
}

// NewCloudEventsEnvelope new a CloudEventsEnvelope wrap with raw data.
func NewCloudEventsEnvelope(id, source, eventType, contentType, subject string, topic, pubsubName string, data []byte) *CloudEventsEnvelope {
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
	if subject == "" {
		subject = DefaultCloudEventSource
	}
	if contentType == "" {
		contentType = DefaultContentType
	}

	return &CloudEventsEnvelope{
		ID:              id,
		SpecVersion:     CloudEventsSpecVersion,
		DataContentType: contentType,
		Source:          source,
		Type:            eventType,
		Subject:         subject,
		Topic:           topic,
		PubsubName:      pubsubName,
		Data:            data,
	}
}
