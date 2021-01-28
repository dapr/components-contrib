// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package contenttype

import "strings"

const (
	// CloudEventContentType is the content type for cloud event.
	CloudEventContentType = "application/cloudevents+json"
	// JSONContentType is the content type for JSON.
	JSONContentType = "application/json"
)

// IsCloudEventContentType checks for content type.
func IsCloudEventContentType(contentType string) bool {
	return isContentType(contentType, CloudEventContentType)
}

// IsJSONContentType checks for content type.
func IsJSONContentType(contentType string) bool {
	return isContentType(contentType, JSONContentType)
}

// IsStringContentType determines if content type is string
func IsStringContentType(contentType string) bool {
	if strings.HasPrefix(strings.ToLower(contentType), "text/") {
		return true
	}

	return isContentType(contentType, "application/xml")
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
