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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTranslateFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		filter  map[string]any
		want    string
		wantErr string
	}{
		{"eq", map[string]any{"tenant": map[string]any{"$eq": "a"}}, `tenant = "a"`, ""},
		{"ne", map[string]any{"tenant": map[string]any{"$ne": "a"}}, `tenant != "a"`, ""},
		{"range", map[string]any{"year": map[string]any{"$gt": 2020, "$lte": 2024}}, `year <= 2024 AND year > 2020`, ""},
		{"in", map[string]any{"tag": map[string]any{"$in": []any{"go", "dapr"}}}, `tag IN ["go", "dapr"]`, ""},
		{"nin", map[string]any{"tag": map[string]any{"$nin": []any{"go"}}}, `NOT (tag IN ["go"])`, ""},
		{"exists", map[string]any{"title": map[string]any{"$exists": true}}, `title EXISTS`, ""},
		{"not exists", map[string]any{"title": map[string]any{"$exists": false}}, `NOT (title EXISTS)`, ""},
		{"and or not", map[string]any{"$and": []any{map[string]any{"a": 1}, map[string]any{"$or": []any{map[string]any{"b": 2}, map[string]any{"$not": map[string]any{"c": 3}}}}}}, `(a = 1) AND ((b = 2) OR (NOT (c = 3)))`, ""},
		{"regex", map[string]any{"title": map[string]any{"$regex": "^a"}}, "", "regular expressions"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := TranslateFilter(tt.filter)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
