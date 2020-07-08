// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

import (
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
)

const (
	// DefaultCloudEventType is the default event type for an Dapr published event
	DefaultCloudEventType = "com.dapr.event.sent"
	// CloudEventsSpecVersion is the specversion used by Dapr for the cloud events implementation
	CloudEventsSpecVersion = "0.3"
	//ContentType is the Cloud Events HTTP content type
	ContentType = "application/cloudevents+json"
	// DefaultCloudEventSource is the default event source
	DefaultCloudEventSource = "Dapr"
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
}

// NewCloudEventsEnvelope returns CloudEventsEnvelope from data or a new one when data content was not
func NewCloudEventsEnvelope(id, source, eventType, subject string, data []byte) *CloudEventsEnvelope {
	var ce CloudEventsEnvelope
	err := jsoniter.Unmarshal(data, &ce)

	if err == nil && ce.ID != "" && ce.SpecVersion != "" && ce.Type != "" && ce.Source != "" && ce.DataContentType != "" {
		// data was already CloudEvent
		// assuming the likelihood of other structures having all these
		//CloudEvent-specific fields is very unlikely
		return &ce
	}

	// ensure valid input parameters
	if id == "" {
		id = uuid.New().String()
	}
	if source == "" {
		source = DefaultCloudEventSource
	}
	if eventType == "" {
		eventType = DefaultCloudEventType
	}

	// create new envelope
	ce = CloudEventsEnvelope{
		ID:              id,
		SpecVersion:     CloudEventsSpecVersion,
		DataContentType: "application/json",
		Source:          source,
		Type:            eventType,
		Subject:         subject,
	}

	// if content was not JSON, set data and its type to text
	if err != nil {
		ce.Data = string(data)
		ce.DataContentType = "text/plain"
	}

	return &ce
}
