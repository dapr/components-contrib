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

	"github.com/dapr/kit/logger"
)

func TestBlobHTTPHeaderGeneration(t *testing.T) {
	log := logger.NewLogger("test")

	t.Run("Content type is set from request, forward compatibility", func(t *testing.T) {
		contentType := "application/json"
		requestMetadata := map[string]string{}

		blobHeaders, err := CreateBlobHTTPHeadersFromRequest(requestMetadata, &contentType, log)
		assert.Nil(t, err)
		assert.Equal(t, "application/json", *blobHeaders.BlobContentType)
	})
	t.Run("Content type and metadata provided (conflict), content type chosen", func(t *testing.T) {
		contentType := "application/json"
		requestMetadata := map[string]string{
			contentTypeKey: "text/plain",
		}

		blobHeaders, err := CreateBlobHTTPHeadersFromRequest(requestMetadata, &contentType, log)
		assert.Nil(t, err)
		assert.Equal(t, "application/json", *blobHeaders.BlobContentType)
	})
	t.Run("ContentType not provided, metadata provided set backward compatibility", func(t *testing.T) {
		requestMetadata := map[string]string{
			contentTypeKey: "text/plain",
		}
		blobHeaders, err := CreateBlobHTTPHeadersFromRequest(requestMetadata, nil, log)
		assert.Nil(t, err)
		assert.Equal(t, "text/plain", *blobHeaders.BlobContentType)
	})
}
