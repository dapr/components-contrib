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

package internal

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	blobstoragecommon "github.com/dapr/components-contrib/common/component/azure/blobstorage"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

func TestInit(t *testing.T) {
	m := state.Metadata{}
	s := &StateStore{
		logger: logger.NewLogger("logger"),
	}

	t.Run("Init with missing metadata", func(t *testing.T) {
		m.Properties = map[string]string{
			"invalidValue": "a",
		}
		err := s.Init(t.Context(), m)
		require.Error(t, err)
		assert.Equal(t, err, errors.New("missing or empty accountName field from metadata"))
	})
}

func TestGetBlobName(t *testing.T) {
	passthrough := func(key string) string { return key }

	t.Run("No prefix configured", func(t *testing.T) {
		s := &StateStore{
			getFileNameFn: passthrough,
		}
		assert.Equal(t, "mykey", s.getBlobName("mykey"))
	})

	t.Run("Prefix without trailing slash", func(t *testing.T) {
		s := &StateStore{
			getFileNameFn: passthrough,
			metadata:      &blobstoragecommon.BlobStorageMetadata{Prefix: "myprefix"},
		}
		assert.Equal(t, "myprefix/mykey", s.getBlobName("mykey"))
	})

	t.Run("Prefix with trailing slash", func(t *testing.T) {
		s := &StateStore{
			getFileNameFn: passthrough,
			metadata:      &blobstoragecommon.BlobStorageMetadata{Prefix: "myprefix/"},
		}
		assert.Equal(t, "myprefix/mykey", s.getBlobName("mykey"))
	})

	t.Run("Prefix with leading slash on key", func(t *testing.T) {
		s := &StateStore{
			getFileNameFn: passthrough,
			metadata:      &blobstoragecommon.BlobStorageMetadata{Prefix: "myprefix"},
		}
		assert.Equal(t, "myprefix/mykey", s.getBlobName("/mykey"))
	})

	t.Run("Prefix combined with custom getFileNameFn", func(t *testing.T) {
		stripAppId := func(key string) string {
			return "stripped-" + key
		}
		s := &StateStore{
			getFileNameFn: stripAppId,
			metadata:      &blobstoragecommon.BlobStorageMetadata{Prefix: "nested/path"},
		}
		assert.Equal(t, "nested/path/stripped-key", s.getBlobName("key"))
	})
}
