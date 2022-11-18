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
	"fmt"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	azauth "github.com/dapr/components-contrib/internal/authentication/azure"
	mdutils "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

type BlobStorageMetadata struct {
	AccountName       string
	AccountKey        string
	ContainerName     string
	RetryCount        int32 `json:"retryCount,string"`
	DecodeBase64      bool  `json:"decodeBase64,string"`
	PublicAccessLevel azblob.PublicAccessType
}

func parseMetadata(meta map[string]string) (*BlobStorageMetadata, error) {
	m := BlobStorageMetadata{
		RetryCount: defaultBlobRetryCount,
	}
	mdutils.DecodeMetadata(meta, &m)

	if val, ok := mdutils.GetMetadataProperty(meta, azauth.StorageAccountNameKeys...); ok && val != "" {
		m.AccountName = val
	} else {
		return nil, fmt.Errorf("missing or empty %s field from metadata", azauth.StorageAccountNameKeys[0])
	}

	if val, ok := mdutils.GetMetadataProperty(meta, azauth.StorageContainerNameKeys...); ok && val != "" {
		m.ContainerName = val
	} else {
		return nil, fmt.Errorf("missing or empty %s field from metadata", azauth.StorageContainerNameKeys[0])
	}

	// per the Dapr documentation "none" is a valid value
	if m.PublicAccessLevel == "none" {
		m.PublicAccessLevel = ""
	}
	if m.PublicAccessLevel != "" && !isValidPublicAccessType(m.PublicAccessLevel) {
		return nil, fmt.Errorf("invalid public access level: %s; allowed: %s",
			m.PublicAccessLevel, azblob.PossiblePublicAccessTypeValues())
	}

	// we need this key for backwards compatibility
	if val, ok := meta["getBlobRetryCount"]; ok && val != "" {
		// convert val from string to int32
		parseInt, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return nil, err
		}
		m.RetryCount = int32(parseInt)
	}

	return &m, nil
}

func isValidPublicAccessType(accessType azblob.PublicAccessType) bool {
	validTypes := azblob.PossiblePublicAccessTypeValues()
	for _, item := range validTypes {
		if item == accessType {
			return true
		}
	}

	return false
}

func SanitizeMetadata(log logger.Logger, metadata map[string]string) map[string]string {
	for key, val := range metadata {
		// Keep only letters and digits
		n := 0
		newKey := make([]byte, len(key))
		for i := 0; i < len(key); i++ {
			if (key[i] >= 'A' && key[i] <= 'Z') ||
				(key[i] >= 'a' && key[i] <= 'z') ||
				(key[i] >= '0' && key[i] <= '9') {
				newKey[n] = key[i]
				n++
			}
		}

		if n != len(key) {
			nks := string(newKey[:n])
			log.Warnf("metadata key %s contains disallowed characters, sanitized to %s", key, nks)
			delete(metadata, key)
			metadata[nks] = val
			key = nks
		}

		// Remove all non-ascii characters
		n = 0
		newVal := make([]byte, len(val))
		for i := 0; i < len(val); i++ {
			if val[i] > 127 {
				continue
			}
			newVal[n] = val[i]
			n++
		}
		metadata[key] = string(newVal[:n])
	}

	return metadata
}
