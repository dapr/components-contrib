// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

import (
	jsoniter "github.com/json-iterator/go"
)

const (
	// DefaultCloudEventType is the default event type for an Dapr published event
	DefaultCloudEventType = "com.dapr.event.sent"
	// CloudEventsSpecVersion is the specversion used by Dapr for the cloud events implementation
	CloudEventsSpecVersion = "0.3"
	//ContentType is the Cloud Events HTTP content type
	ContentType = "application/json"
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
// already an existing CE. Invoked by either the HTTP or gRPC pug handles where:
//  id = new UUID
// 	source = handling app ID
//  eventType = const DefaultCloudEventType from this package ("com.dapr.event.sent")
//  subject = diagnostics span context W3C as string
func NewCloudEventsEnvelope(id, source, eventType, subject string, data []byte) *CloudEventsEnvelope {
	var ce CloudEventsEnvelope
	err := jsoniter.Unmarshal(data, &ce)
	if err != nil {
		// data content is not JSON
		ce = CloudEventsEnvelope{
			Data:            string(data),
			DataContentType: "text/plain",
		}
	}

	// populate the values where CloudEvent has none
	if ce.ID == "" {
		ce.ID = id
	}

	if ce.Source == "" {
		ce.Source = source
	}

	if ce.Type == "" {
		if eventType == "" {
			ce.Type = DefaultCloudEventType
		} else {
			ce.Type = eventType
		}
	}

	if ce.SpecVersion == "" {
		ce.SpecVersion = CloudEventsSpecVersion
	}

	if ce.DataContentType == "" {
		ce.DataContentType = ContentType
	}

	if ce.Data == nil {
		ce.Data = data
	}

	if ce.Subject == "" {
		ce.Subject = subject
	}

	return &ce
}
