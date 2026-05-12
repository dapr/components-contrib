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
	"encoding/json"
	"fmt"
	"sort"

	meilisearchgo "github.com/meilisearch/meilisearch-go"

	"github.com/dapr/components-contrib/search"
)

// NormalizeAck preserves supported acknowledgement modes.
func NormalizeAck(ack search.IndexAck) search.IndexAck {
	switch ack {
	case search.IndexAckUnspecified, search.IndexAckQueued, search.IndexAckDurable:
		return ack
	default:
		return search.IndexAckUnspecified
	}
}

// WaitForTask waits for a Meilisearch task to finish successfully.
func WaitForTask(ctx context.Context, client meilisearchgo.ServiceManager, taskUID int64) error {
	task, err := client.WaitForTaskWithContext(ctx, taskUID, WaitInterval)
	if err != nil {
		return fmt.Errorf("wait for meilisearch task %d: %w", taskUID, err)
	}
	if task.Status != meilisearchgo.TaskStatusSucceeded {
		msg := task.Error.Message
		if msg == "" {
			msg = string(task.Status)
		}
		return fmt.Errorf("meilisearch task %d failed: %s", taskUID, msg)
	}
	return nil
}

// MarkFailed marks all operation results as failed with the provided error.
func MarkFailed(results []search.OperationResult, err error) {
	for i := range results {
		results[i].Success = false
		results[i].ErrorCode = "task_failed"
		results[i].ErrorMessage = err.Error()
	}
}

// SettingsFromFields converts Dapr field schema into Meilisearch settings.
func SettingsFromFields(fields []search.IndexFieldSchema) *meilisearchgo.Settings {
	settings := &meilisearchgo.Settings{}
	for _, f := range fields {
		if f.Filterable {
			settings.FilterableAttributes = append(settings.FilterableAttributes, f.Name)
		}
		if f.Sortable {
			settings.SortableAttributes = append(settings.SortableAttributes, f.Name)
		}
		if f.Searchable {
			settings.SearchableAttributes = append(settings.SearchableAttributes, f.Name)
		}
	}
	return settings
}

// FieldsFromSettings converts Meilisearch settings into Dapr field schema.
func FieldsFromSettings(settings *meilisearchgo.Settings) []search.IndexFieldSchema {
	byName := map[string]*search.IndexFieldSchema{}
	ensure := func(name string) *search.IndexFieldSchema {
		if byName[name] == nil {
			byName[name] = &search.IndexFieldSchema{Name: name, Type: search.IndexFieldTypeUnspecified}
		}
		return byName[name]
	}
	for _, name := range settings.FilterableAttributes {
		ensure(name).Filterable = true
	}
	for _, name := range settings.SortableAttributes {
		ensure(name).Sortable = true
	}
	for _, name := range settings.SearchableAttributes {
		if name == "*" {
			continue
		}
		ensure(name).Searchable = true
	}
	fields := make([]search.IndexFieldSchema, 0, len(byName))
	for _, f := range byName {
		fields = append(fields, *f)
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i].Name < fields[j].Name })
	return fields
}

// NumberAsFloat converts numeric JSON values into float64.
func NumberAsFloat(value any) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case json.Number:
		f, _ := v.Float64()
		return f
	default:
		return 0
	}
}

// CloneMap copies a document or metadata map with capacity for component fields.
func CloneMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in)+2)
	for k, v := range in {
		out[k] = v
	}
	return out
}

// EncodeContinuationToken encodes a Meilisearch offset token.
func EncodeContinuationToken(offset int64) string {
	b, _ := json.Marshal(map[string]int64{"offset": offset})
	return string(b)
}

// DecodeContinuationToken decodes a Meilisearch offset token.
func DecodeContinuationToken(token string) (int64, error) {
	var value struct {
		Offset int64 `json:"offset"`
	}
	if err := json.Unmarshal([]byte(token), &value); err != nil {
		return 0, fmt.Errorf("decode continuation token: %w", err)
	}
	return value.Offset, nil
}
