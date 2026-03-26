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

// Avro schema structs with deterministic JSON field ordering.
// Using structs instead of maps guarantees consistent json.Marshal output
// across process restarts, preventing spurious schema version bumps in the
// Pulsar Schema Registry (which hashes raw schema bytes for versioning).

type avroRecordSchema struct {
	Type      string        `json:"type"`
	Name      string        `json:"name"`
	Namespace string        `json:"namespace"`
	Fields    []interface{} `json:"fields"`
}

type avroRequiredField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type avroNullableField struct {
	Name    string `json:"name"`
	Type    [2]any `json:"type"`
	Default any    `json:"default"` // nil any marshals to JSON null
}

// wrapInCloudEventsAvroSchema takes a user-provided inner Avro schema JSON string
// and returns a CloudEvents envelope Avro schema with the inner schema embedded
// as the "data" field type. This ensures the Pulsar Schema Registry entry matches
// the actual wire format when Dapr wraps payloads in CloudEvents envelopes.
//
// The generated schema follows the CloudEvents Avro format specification:
// https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/avro-format.md
func wrapInCloudEventsAvroSchema(innerSchemaJSON string) (string, error) {
	var innerSchema interface{}
	if err := json.Unmarshal([]byte(innerSchemaJSON), &innerSchema); err != nil {
		return "", fmt.Errorf("failed to parse inner Avro schema: %w", err)
	}

	nullStr := [2]any{"null", "string"}

	envelope := avroRecordSchema{
		Type:      "record",
		Name:      "CloudEvent",
		Namespace: "io.cloudevents",
		Fields: []interface{}{
			avroRequiredField{Name: "id", Type: "string"},
			avroRequiredField{Name: "source", Type: "string"},
			avroRequiredField{Name: "specversion", Type: "string"},
			avroRequiredField{Name: "type", Type: "string"},
			avroNullableField{Name: "datacontenttype", Type: nullStr},
			avroNullableField{Name: "subject", Type: nullStr},
			avroNullableField{Name: "time", Type: nullStr},
			avroNullableField{Name: "topic", Type: nullStr},
			avroNullableField{Name: "pubsubname", Type: nullStr},
			avroNullableField{Name: "traceid", Type: nullStr},
			avroNullableField{Name: "traceparent", Type: nullStr},
			avroNullableField{Name: "tracestate", Type: nullStr},
			avroNullableField{Name: "expiration", Type: nullStr},
			avroNullableField{Name: "data", Type: [2]any{"null", innerSchema}},
			avroNullableField{Name: "data_base64", Type: nullStr},
		},
	}

	result, err := json.Marshal(envelope)
	if err != nil {
		return "", fmt.Errorf("failed to marshal CloudEvents envelope schema: %w", err)
	}

	return string(result), nil
}

// normalizeCloudEventForAvro takes a Dapr-produced CE envelope JSON and parses
// the stringified "data" field into a proper JSON object so goavro can encode it
// against the inner Avro record type.
//
// Dapr produces: {"data": "{\"testId\":0}", ...}
// goavro expects: {"data": {"testId":0}, ...}
//
// Uses json.RawMessage to avoid parsing/re-serialising the other ~15 CE fields.
func normalizeCloudEventForAvro(ceJSON []byte) ([]byte, error) {
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
