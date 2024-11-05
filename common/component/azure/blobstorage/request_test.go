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

package blobstorage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/kit/logger"
)

func TestBlobHTTPHeaderGeneration(t *testing.T) {
	log := logger.NewLogger("test")

	t.Run("Content type is set from request, forward compatibility", func(t *testing.T) {
		contentType := "application/json"
		requestMetadata := map[string]string{}

		blobHeaders, err := CreateBlobHTTPHeadersFromRequest(requestMetadata, &contentType, log)
		require.NoError(t, err)
		assert.Equal(t, "application/json", *blobHeaders.BlobContentType)
	})
	t.Run("Content type and metadata provided (conflict), content type chosen", func(t *testing.T) {
		contentType := "application/json"
		requestMetadata := map[string]string{
			contentTypeKey: "text/plain",
		}

		blobHeaders, err := CreateBlobHTTPHeadersFromRequest(requestMetadata, &contentType, log)
		require.NoError(t, err)
		assert.Equal(t, "application/json", *blobHeaders.BlobContentType)
	})
	t.Run("ContentType not provided, metadata provided set backward compatibility", func(t *testing.T) {
		requestMetadata := map[string]string{
			contentTypeKey: "text/plain",
		}
		blobHeaders, err := CreateBlobHTTPHeadersFromRequest(requestMetadata, nil, log)
		require.NoError(t, err)
		assert.Equal(t, "text/plain", *blobHeaders.BlobContentType)
	})
}

func TestSanitizeRequestMetadata(t *testing.T) {
	log := logger.NewLogger("test")
	t.Run("sanitize metadata if necessary", func(t *testing.T) {
		m := map[string]string{
			"somecustomfield": "some-custom-value",
			"specialfield":    "special:valueÜ",
			"not-allowed:":    "not-allowed",
			"ctr-characters":  string([]byte{72, 20, 1, 0, 101, 120}),
		}
		meta := SanitizeMetadata(log, m)
		_ = assert.NotNil(t, meta["somecustomfield"]) &&
			assert.Equal(t, "some-custom-value", *meta["somecustomfield"])
		_ = assert.NotNil(t, meta["specialfield"]) &&
			assert.Equal(t, "special:value", *meta["specialfield"])
		_ = assert.NotNil(t, meta["notallowed"]) &&
			assert.Equal(t, "not-allowed", *meta["notallowed"])
		_ = assert.NotNil(t, meta["ctrcharacters"]) &&
			assert.Equal(t, string([]byte{72, 101, 120}), *meta["ctrcharacters"])
	})
}

func TestIsLWS(t *testing.T) {
	// Test cases for isLWS
	tests := []struct {
		input    byte
		expected bool
	}{
		{' ', true},   // Space character, should return true
		{'\t', true},  // Tab character, should return true
		{'A', false},  // Non-LWS character, should return false
		{'1', false},  // Non-LWS character, should return false
		{'\n', false}, // Newline, a CTL but not LWS, should return false
		{0x7F, false}, // DEL character, a CTL but not LWS, should return false
	}

	for _, tt := range tests {
		t.Run("Testing for LWS", func(t *testing.T) {
			result := isLWS(tt.input)
			assert.Equal(t, tt.expected, result, "input: %v", tt.input)
		})
	}
}

func TestIsCTL(t *testing.T) {
	// Test cases for isCTL
	tests := []struct {
		input    byte
		expected bool
	}{
		{0x00, true}, // NUL, a control character
		{0x1F, true}, // US (Unit Separator), a control character
		{'\n', true}, // Newline, a control character
		{0x7F, true}, // DEL, a control character
		{'A', false}, // Non-CTL character
		{'1', false}, // Non-CTL character
		{' ', false}, // Space is not a CTL (although LWS)
	}

	for _, tt := range tests {
		t.Run("Testing for CTL characters", func(t *testing.T) {
			result := isCTL(tt.input)
			assert.Equal(t, tt.expected, result, "input: %v", tt.input)
		})
	}
}
