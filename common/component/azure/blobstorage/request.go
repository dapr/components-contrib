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
	"errors"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"

	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

const (
	contentTypeKey        = "contenttype"
	contentMD5Key         = "contentmd5"
	contentEncodingKey    = "contentencoding"
	contentLanguageKey    = "contentlanguage"
	contentDispositionKey = "contentdisposition"
	cacheControlKey       = "cachecontrol"
)

func CreateBlobHTTPHeadersFromRequest(meta map[string]string, contentType *string, log logger.Logger) (blob.HTTPHeaders, error) {
	// build map to support arbitrary case
	caseMap := make(map[string]string)
	for k := range meta {
		caseMap[strings.ToLower(k)] = k
	}

	blobHTTPHeaders := blob.HTTPHeaders{}
	if val, ok := meta[caseMap[contentTypeKey]]; ok && val != "" {
		blobHTTPHeaders.BlobContentType = &val
		delete(meta, caseMap[contentTypeKey])
	}

	if contentType != nil {
		if blobHTTPHeaders.BlobContentType != nil {
			log.Warnf("ContentType received from request Metadata %s, as well as ContentType property %s, choosing value from contentType property", blobHTTPHeaders.BlobContentType, *contentType)
		}
		blobHTTPHeaders.BlobContentType = contentType
	}

	if val, ok := meta[caseMap[contentMD5Key]]; ok && val != "" {
		sDec, err := b64.StdEncoding.DecodeString(val)
		if err != nil || len(sDec) != 16 {
			return blob.HTTPHeaders{}, errors.New("the MD5 value specified in Content MD5 is invalid, MD5 value must be 128 bits and base64 encoded")
		}
		blobHTTPHeaders.BlobContentMD5 = sDec
		delete(meta, caseMap[contentMD5Key])
	}
	if val, ok := meta[caseMap[contentEncodingKey]]; ok && val != "" {
		blobHTTPHeaders.BlobContentEncoding = &val
		delete(meta, caseMap[contentEncodingKey])
	}
	if val, ok := meta[caseMap[contentLanguageKey]]; ok && val != "" {
		blobHTTPHeaders.BlobContentLanguage = &val
		delete(meta, caseMap[contentLanguageKey])
	}
	if val, ok := meta[caseMap[contentDispositionKey]]; ok && val != "" {
		blobHTTPHeaders.BlobContentDisposition = &val
		delete(meta, caseMap[contentDispositionKey])
	}
	if val, ok := meta[caseMap[cacheControlKey]]; ok && val != "" {
		blobHTTPHeaders.BlobCacheControl = &val
		delete(meta, caseMap[cacheControlKey])
	}
	return blobHTTPHeaders, nil
}

// SanitizeMetadata is used by Azure Blob Storage components to sanitize the metadata.
// Keys can only contain [A-Za-z0-9], and values are only allowed characters in the ASCII table.
func SanitizeMetadata(log logger.Logger, metadata map[string]string) map[string]*string {
	res := make(map[string]*string, len(metadata))
	for key, val := range metadata {
		// Keep only letters and digits
		n := 0
		newKey := make([]byte, len(key))
		for i := range len(key) {
			if (key[i] >= 'A' && key[i] <= 'Z') ||
				(key[i] >= 'a' && key[i] <= 'z') ||
				(key[i] >= '0' && key[i] <= '9') {
				newKey[n] = key[i]
				n++
			}
		}

		if n != len(key) {
			nks := string(newKey[:n])
			log.Debugf("metadata key %s contains disallowed characters, sanitized to %s", key, nks)
			key = nks
		}

		// Remove all non-ascii characters
		n = 0
		newVal := make([]byte, len(val))
		for i := range len(val) {
			if val[i] > 127 || val[i] == 0 {
				continue
			}
			newVal[n] = val[i]
			n++
		}
		res[key] = ptr.Of(string(newVal[:n]))
	}

	return res
}
