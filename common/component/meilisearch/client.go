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

package meilisearch

import (
	"errors"
	"net/http"
	"strings"
	"time"

	meilisearchgo "github.com/meilisearch/meilisearch-go"
)

const (
	// PrimaryKey is the Meilisearch primary key field used by Dapr search and vector components.
	PrimaryKey = "id"
	// WaitInterval is the polling interval used while waiting for Meilisearch tasks.
	WaitInterval = 50 * time.Millisecond
)

// NewClient creates a Meilisearch service manager from component metadata.
func NewClient(md MeilisearchMetadata) (meilisearchgo.ServiceManager, error) {
	if strings.TrimSpace(md.Host) == "" {
		return nil, errors.New("meilisearch host is required")
	}

	opts := []meilisearchgo.Option{}
	if md.APIKey != "" {
		opts = append(opts, meilisearchgo.WithAPIKey(md.APIKey))
	}
	if md.Timeout != nil {
		opts = append(opts, meilisearchgo.WithCustomClient(&http.Client{Timeout: *md.Timeout}))
	}

	return meilisearchgo.New(md.Host, opts...), nil
}
