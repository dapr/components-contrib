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
		{"eq", map[string]any{"tenant": map[string]any{"$eq": "a"}}, `"tenant" = "a"`, ""},
		{"ne", map[string]any{"tenant": map[string]any{"$ne": "a"}}, `"tenant" != "a"`, ""},
		{"range", map[string]any{"year": map[string]any{"$gt": 2020, "$lte": 2024}}, `"year" <= 2024 AND "year" > 2020`, ""},
		{"in", map[string]any{"tag": map[string]any{"$in": []any{"go", "dapr"}}}, `"tag" IN ["go", "dapr"]`, ""},
		{"nin", map[string]any{"tag": map[string]any{"$nin": []any{"go"}}}, `NOT ("tag" IN ["go"])`, ""},
		{"exists", map[string]any{"title": map[string]any{"$exists": true}}, `"title" EXISTS`, ""},
		{"not exists", map[string]any{"title": map[string]any{"$exists": false}}, `NOT ("title" EXISTS)`, ""},
		{"and or not", map[string]any{"$and": []any{map[string]any{"a": 1}, map[string]any{"$or": []any{map[string]any{"b": 2}, map[string]any{"$not": map[string]any{"c": 3}}}}}}, `("a" = 1) AND (("b" = 2) OR (NOT ("c" = 3)))`, ""},
		{"nested path", map[string]any{"publisher.city": "london"}, `"publisher.city" = "london"`, ""},
		// The runtime decodes every JSON number as float64; Meilisearch does not
		// parse the exponent form Go prints for large floats.
		{"large float", map[string]any{"price": map[string]any{"$gte": float64(1000000)}}, `"price" >= 1000000`, ""},
		{"fractional float", map[string]any{"price": 12.5}, `"price" = 12.5`, ""},
		{"float epoch", map[string]any{"at": map[string]any{"$lt": 1.7e12}}, `"at" < 1700000000000`, ""},
		{"floats in a list", map[string]any{"year": map[string]any{"$in": []any{float64(1), 2e6}}}, `"year" IN [1, 2000000]`, ""},
		// AND binds tighter than OR, so a compound that shares its object with
		// other conditions must not absorb or shed them.
		{"or beside a field", map[string]any{"tenant": "acme", "$or": []any{map[string]any{"a": 1}, map[string]any{"b": 2}}}, `"tenant" = "acme" AND (("a" = 1) OR ("b" = 2))`, ""},
		{"or beside and", map[string]any{"$and": []any{map[string]any{"a": 1}}, "$or": []any{map[string]any{"b": 2}, map[string]any{"c": 3}}}, `(("a" = 1)) AND (("b" = 2) OR ("c" = 3))`, ""},
		// A field name is caller data and must not be read as filter syntax.
		{"field name with filter syntax", map[string]any{`tenant = "acme" OR 1 = 1 OR "x`: "y"}, `"tenant = \"acme\" OR 1 = 1 OR \"x" = "y"`, ""},
		{"field name with a space", map[string]any{"release date": map[string]any{"$exists": true}}, `"release date" EXISTS`, ""},
		{"regex", map[string]any{"title": map[string]any{"$regex": "^a"}}, "", "regular expressions"},
		{"unknown top-level operator", map[string]any{"$near": map[string]any{"a": 1}}, "", `unsupported filter operator "$near"`},
		{"unknown field operator", map[string]any{"title": map[string]any{"$like": "a"}}, "", `unsupported filter operator "$like"`},
		{"in without an array", map[string]any{"tag": map[string]any{"$in": "go"}}, "", "$in requires an array"},
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

func TestTranslateFilterWithPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		filter  map[string]any
		want    string
		wantErr string
	}{
		{"eq", map[string]any{"author": "jane-austen"}, `"daprMetadata.author" = "jane-austen"`, ""},
		{"nested path", map[string]any{"publisher.city": map[string]any{"$ne": "london"}}, `"daprMetadata.publisher.city" != "london"`, ""},
		{"in", map[string]any{"tag": map[string]any{"$in": []any{"go", 1, true}}}, `"daprMetadata.tag" IN ["go", 1, true]`, ""},
		{"exists", map[string]any{"title": map[string]any{"$exists": false}}, `NOT ("daprMetadata.title" EXISTS)`, ""},
		{"logical operators are not prefixed", map[string]any{"$or": []any{map[string]any{"a": 1}, map[string]any{"$not": map[string]any{"b": 2}}}}, `("daprMetadata.a" = 1) OR (NOT ("daprMetadata.b" = 2))`, ""},
		{"unknown operator", map[string]any{"$near": map[string]any{"a": 1}}, "", `unsupported filter operator "$near"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := TranslateFilterWithPrefix(tt.filter, MetadataField+".")
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
