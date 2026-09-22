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
	"context"
	"errors"
	"net/http"
	"strings"

	meilisearchgo "github.com/meilisearch/meilisearch-go"
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

// WaitForTask waits for one of the Meilisearch tasks backing a synchronous
// index or collection lifecycle operation and converts a non-successful task
// into a gRPC status error. Document and vector writes go through the
// TaskDispatcher instead so they honour IndexingOptions.
func WaitForTask(ctx context.Context, tasks meilisearchgo.TaskReader, taskUID int64, msg string) error {
	task, err := tasks.WaitForTaskWithContext(ctx, taskUID, WaitInterval)
	if err != nil {
		return StatusError(err, msg)
	}
	change := TaskChange{UID: taskUID, Status: task.Status, Type: task.Type, IndexUID: task.IndexUID}
	if task.Status == meilisearchgo.TaskStatusFailed {
		change.Error = taskAPIError(task)
	}
	if statusErr := change.TaskStatusError(); statusErr != nil {
		return statusErr
	}
	return nil
}
