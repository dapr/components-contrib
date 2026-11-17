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
	"time"

	"github.com/stretchr/testify/require"

	kmeta "github.com/dapr/kit/metadata"
)

func TestDecodeMetadata(t *testing.T) {
	t.Parallel()

	md := MeilisearchMetadata{}
	err := kmeta.DecodeMetadata(map[string]string{"host": "http://localhost:7700", "apiKey": "masterKey", "timeout": "5s"}, &md)
	require.NoError(t, err)
	require.Equal(t, "http://localhost:7700", md.Host)
	require.Equal(t, "masterKey", md.APIKey)
	require.NotNil(t, md.Timeout)
	require.Equal(t, 5*time.Second, *md.Timeout)
}
