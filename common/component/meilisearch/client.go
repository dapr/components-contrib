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
	"fmt"
	"net/http"
	"strings"
	"time"

	meilisearchgo "github.com/meilisearch/meilisearch-go"

	"github.com/dapr/kit/logger"
)

const (
	// indexPageSize is the page size used to list the indexes of an instance.
	indexPageSize = 100

	// rollbackTimeout bounds the deletion of an index whose creation could not
	// be completed.
	rollbackTimeout = 30 * time.Second
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

// ListIndexUIDs returns the UID of every index of the instance. Meilisearch
// pages the index list (20 indexes by default), so it is read page by page
// until the reported total is reached.
func ListIndexUIDs(ctx context.Context, client meilisearchgo.ServiceManager) ([]string, error) {
	uids := []string{}
	for offset := int64(0); ; {
		page, err := client.ListIndexesWithContext(ctx, &meilisearchgo.IndexesQuery{Limit: indexPageSize, Offset: offset})
		if err != nil {
			return nil, err
		}
		for _, idx := range page.Results {
			uids = append(uids, idx.UID)
		}
		offset += int64(len(page.Results))
		if len(page.Results) == 0 || offset >= page.Total {
			return uids, nil
		}
	}
}

// RollbackIndex deletes an index that the current request created but could
// not finish configuring. Left behind, the index would make every retry fail
// with ALREADY_EXISTS without ever applying the requested settings. The
// request context may be what ended the request, so the deletion runs on a
// detached, time-bounded context. A failed rollback is logged: the caller
// reports the error that caused it.
func RollbackIndex(ctx context.Context, client meilisearchgo.ServiceManager, uid string, log logger.Logger) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackTimeout)
	defer cancel()

	task, err := client.DeleteIndexWithContext(cleanupCtx, uid)
	if err == nil {
		err = WaitForTask(cleanupCtx, client, task.TaskUID, fmt.Sprintf("delete meilisearch index %q", uid))
	}
	if err != nil && log != nil {
		log.Warnf("meilisearch: index %q was created but could not be configured, and removing it failed: %v", uid, err)
	}
}
