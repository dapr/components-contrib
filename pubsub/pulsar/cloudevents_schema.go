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

package pulsar

import (
	"encoding/json"
	"fmt"
)

// CloudEvents envelope schema structs with deterministic JSON field ordering.
// Used for both Avro and JSON schema topics. Using structs instead of maps
// guarantees consistent json.Marshal output across process restarts, preventing
// spurious schema version bumps in the Pulsar Schema Registry (which hashes
// raw schema bytes for versioning).

type ceRecordSchema struct {
	Type      string        `json:"type"`
	Name      string        `json:"name"`
	Namespace string        `json:"namespace"`
	Fields    []interface{} `json:"fields"`
}

type ceRequiredField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type ceNullableField struct {
	Name    string `json:"name"`
	Type    [2]any `json:"type"`
	Default any    `json:"default"` // nil any marshals to JSON null
}

// wrapInCloudEventsSchema takes a user-provided inner schema JSON string and
// returns a CloudEvents envelope schema with the inner schema embedded as the
// "data" field type. This ensures the Pulsar Schema Registry entry matches the
// actual wire format when Dapr wraps payloads in CloudEvents envelopes.
//
// Used for both Avro and JSON schema topics — the envelope structure is the same
// regardless of the schema type because both use the Avro-compatible JSON
// representation internally (goavro codec).
//
// The generated schema follows the CloudEvents Avro format specification:
// https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/avro-format.md
func wrapInCloudEventsSchema(innerSchemaJSON string) (string, error) {
	var innerSchema interface{}
	if err := json.Unmarshal([]byte(innerSchemaJSON), &innerSchema); err != nil {
		return "", fmt.Errorf("failed to parse inner schema: %w", err)
	}

	nullStr := [2]any{"null", "string"}

	envelope := ceRecordSchema{
		Type:      "record",
		Name:      "CloudEvent",
		Namespace: "io.cloudevents",
		Fields: []interface{}{
			ceRequiredField{Name: "id", Type: "string"},
			ceRequiredField{Name: "source", Type: "string"},
			ceRequiredField{Name: "specversion", Type: "string"},
			ceRequiredField{Name: "type", Type: "string"},
			ceNullableField{Name: "datacontenttype", Type: nullStr},
			ceNullableField{Name: "subject", Type: nullStr},
			ceNullableField{Name: "time", Type: nullStr},
			ceNullableField{Name: "topic", Type: nullStr},
			ceNullableField{Name: "pubsubname", Type: nullStr},
			ceNullableField{Name: "traceid", Type: nullStr},
			ceNullableField{Name: "traceparent", Type: nullStr},
			ceNullableField{Name: "tracestate", Type: nullStr},
			ceNullableField{Name: "expiration", Type: nullStr},
			ceNullableField{Name: "data", Type: [2]any{"null", innerSchema}},
			ceNullableField{Name: "data_base64", Type: nullStr},
		},
	}

	result, err := json.Marshal(envelope)
	if err != nil {
		return "", fmt.Errorf("failed to marshal CloudEvents envelope schema: %w", err)
	}

	return string(result), nil
}

// normalizeCloudEventData takes a Dapr-produced CE envelope JSON and parses
// the stringified "data" field into a proper JSON object so goavro can encode it
// against the inner record type.
//
// Used for both Avro and JSON schema topics — Dapr may stringify the data field
// in either case, and the goavro codec expects a nested object.
//
// Dapr produces: {"data": "{\"testId\":0}", ...}
// goavro expects: {"data": {"testId":0}, ...}
//
// Uses json.RawMessage to avoid fully parsing the other ~15 CE fields, though
// the envelope is re-marshaled (which may reorder top-level keys).
func normalizeCloudEventData(ceJSON []byte) ([]byte, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(ceJSON, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse CloudEvents envelope: %w", err)
	}

	dataRaw := raw["data"]
	if len(dataRaw) == 0 || dataRaw[0] != '"' {
		// data is null, an object, or absent — no normalization needed.
		return ceJSON, nil
	}

	// data is a JSON string — unquote to get the inner JSON bytes.
	var innerStr string
	if err := json.Unmarshal(dataRaw, &innerStr); err != nil {
		return nil, fmt.Errorf("failed to unquote stringified data field: %w", err)
	}

	// Validate the inner JSON is well-formed before embedding it.
	if !json.Valid([]byte(innerStr)) {
		return nil, fmt.Errorf("data field contains invalid JSON: %s", innerStr)
	}

	raw["data"] = json.RawMessage(innerStr)

	return json.Marshal(raw)
}
