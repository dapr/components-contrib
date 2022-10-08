/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
tITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package contenttype

import "strings"

const (
	// CloudEventContentType is the content type for cloud event.
	CloudEventContentType = "application/cloudevents+json"
	// JSONContentType is the content type for JSON.
	JSONContentType = "application/json"
	// GRPCContentType is the content type for grpc.
	GRPCContentType = "application/grpc"
	// XMLContentType is the content type for xml.
	XMLContentType = "application/xml"
	// OctetStreamContentType is the content type for octet-stream.
	OctetStreamContentType = "application/octet-stream"
)

// IsCloudEventContentType checks for content type.
func IsCloudEventContentType(contentType string) bool {
	return isContentType(contentType, CloudEventContentType)
}

// IsJSONContentType checks for content type.
func IsJSONContentType(contentType string) bool {
	return isContentType(contentType, JSONContentType)
}

// IsGRPCContentType checks for content type.
func IsGRPCContenttype(contentType string) bool {
	return isContentType(contentType, GRPCContentType)
}

// IsStringContentType determines if content type is string.
func IsStringContentType(contentType string) bool {
	if strings.HasPrefix(strings.ToLower(contentType), "text/") {
		return true
	}

	return isContentType(contentType, XMLContentType)
}

// IsBinaryContentType determines if content type is byte[].
func IsBinaryContentType(contentType string) bool {
	return isContentType(contentType, OctetStreamContentType)
}

func isContentType(contentType string, expected string) bool {
	lowerContentType := strings.ToLower(contentType)
	if lowerContentType == expected {
		return true
	}

	semiColonPos := strings.Index(lowerContentType, ";")
	if semiColonPos >= 0 {
		return lowerContentType[0:semiColonPos] == expected
	}

	return false
}
