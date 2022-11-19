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
	b64 "encoding/base64"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"

	"github.com/dapr/kit/logger"
)

const (
	contentTypeKey        = "ContentType"
	contentMD5Key         = "ContentMD5"
	contentEncodingKey    = "ContentEncoding"
	contentLanguageKey    = "ContentLanguage"
	contentDispositionKey = "ContentDisposition"
	cacheControlKey       = "CacheControl"
)

func CreateBlobHTTPHeadersFromRequest(meta map[string]string, contentType *string, log logger.Logger) (blob.HTTPHeaders, error) {
	blobHTTPHeaders := blob.HTTPHeaders{}
	if val, ok := meta[contentTypeKey]; ok && val != "" {
		blobHTTPHeaders.BlobContentType = &val
		delete(meta, contentTypeKey)
	}

	if contentType != nil {
		if blobHTTPHeaders.BlobContentType != nil {
			log.Warnf("ContentType received from request Metadata %s, as well as ContentType property %s, choosing value from contentType property", blobHTTPHeaders.BlobContentType, *contentType)
		}
		blobHTTPHeaders.BlobContentType = contentType
	}

	if val, ok := meta[contentMD5Key]; ok && val != "" {
		sDec, err := b64.StdEncoding.DecodeString(val)
		if err != nil || len(sDec) != 16 {
			return blob.HTTPHeaders{}, fmt.Errorf("the MD5 value specified in Content MD5 is invalid, MD5 value must be 128 bits and base64 encoded")
		}
		blobHTTPHeaders.BlobContentMD5 = sDec
		delete(meta, contentMD5Key)
	}
	if val, ok := meta[contentEncodingKey]; ok && val != "" {
		blobHTTPHeaders.BlobContentEncoding = &val
		delete(meta, contentEncodingKey)
	}
	if val, ok := meta[contentLanguageKey]; ok && val != "" {
		blobHTTPHeaders.BlobContentLanguage = &val
		delete(meta, contentLanguageKey)
	}
	if val, ok := meta[contentDispositionKey]; ok && val != "" {
		blobHTTPHeaders.BlobContentDisposition = &val
		delete(meta, contentDispositionKey)
	}
	if val, ok := meta[cacheControlKey]; ok && val != "" {
		blobHTTPHeaders.BlobCacheControl = &val
		delete(meta, cacheControlKey)
	}
	return blobHTTPHeaders, nil
}
