/*
Copyright 2025 The Dapr Authors
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

// Package datalake provides shared helpers for building Azure Data Lake
// Storage Gen2 (ADLS Gen2) filesystem clients from Dapr component metadata.
// It mirrors the structure of common/component/azure/blobstorage.
package datalake

import (
	"fmt"

	azauth "github.com/dapr/components-contrib/common/authentication/azure"
	mdutils "github.com/dapr/components-contrib/metadata"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	// Specifies the maximum number of HTTP requests that will be made to retry filesystem operations. A value
	// of zero means that no additional HTTP requests will be made.
	defaultDataLakeRetryCount = 3
)

// DataLakeMetadata contains the parsed metadata used to construct an ADLS
// Gen2 filesystem client.
type DataLakeMetadata struct {
	FileSystemClientOpts    `json:",inline" mapstructure:",squash"`
	DisableEntityManagement bool `json:"disableEntityManagement,string" mapstructure:"disableEntityManagement"`
}

// FileSystemClientOpts contains the connection options for an ADLS Gen2
// filesystem client.
type FileSystemClientOpts struct {
	// Use a connection string
	ConnectionString string
	FileSystemName   string
	Prefix           string `json:"prefix" mapstructure:"prefix"`

	// Use a shared account key
	AccountName string
	AccountKey  string

	// Misc
	RetryCount int32 `json:"retryCount,string"`

	// Private properties
	customEndpoint string `json:"-" mapstructure:"-"`
}

func parseMetadata(meta map[string]string) (*DataLakeMetadata, error) {
	m := DataLakeMetadata{}
	m.RetryCount = defaultDataLakeRetryCount
	decodeErr := kitmd.DecodeMetadata(meta, &m)
	if decodeErr != nil {
		return nil, fmt.Errorf("failed to decode metadata: %w", decodeErr)
	}

	if m.ConnectionString == "" {
		if val, ok := mdutils.GetMetadataProperty(meta, azauth.MetadataKeys["StorageAccountName"]...); ok && val != "" {
			m.AccountName = val
		} else {
			return nil, fmt.Errorf("missing or empty %s field from metadata", azauth.MetadataKeys["StorageAccountName"][0])
		}
	}

	if val, ok := mdutils.GetMetadataProperty(meta, "fileSystemName", "fileSystem", "filesystemName"); ok && val != "" {
		m.FileSystemName = val
	} else {
		return nil, fmt.Errorf("missing or empty fileSystemName field from metadata")
	}

	if val, ok := mdutils.GetMetadataProperty(meta, azauth.MetadataKeys["StorageAccountKey"]...); ok && val != "" {
		m.AccountKey = val
	}

	return &m, nil
}
