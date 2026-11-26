//go:build integration_test
// +build integration_test

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

package bucket

import (
	"os"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"

	"github.com/dapr/components-contrib/binarystore"
	"github.com/dapr/components-contrib/binarystore/internal/storetest"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func TestStoreBehaviourWithFloci(t *testing.T) {
	require.NotEmpty(t, os.Getenv("STORAGE_EMULATOR_HOST"), "the integration test must target a GCS emulator")
	project := os.Getenv("GOOGLE_CLOUD_PROJECT")
	require.NotEmpty(t, project)

	admin, err := storage.NewClient(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, admin.Close())
	})

	storetest.RunSuite(t, func(t *testing.T, prefix string) storetest.Harness {
		bucketName := "dapr-binarystore-" + uuid.NewString()
		require.NoError(t, admin.Bucket(bucketName).Create(t.Context(), project, nil))

		store := NewGCPBucket(logger.NewLogger("test"))
		require.NoError(t, store.Init(t.Context(), binarystore.Metadata{
			Base: contribMetadata.Base{
				Properties: map[string]string{
					"bucket": bucketName,
					"prefix": prefix,
				},
			},
		}))
		t.Cleanup(func() {
			_ = store.Close()
		})

		return storetest.Harness{
			Store:  store,
			Prefix: prefix,
			Names: func() []string {
				return objectNames(t, admin, bucketName)
			},
		}
	})
}

func objectNames(t *testing.T, client *storage.Client, bucket string) []string {
	t.Helper()

	names := make([]string, 0)
	objects := client.Bucket(bucket).Objects(t.Context(), nil)
	for {
		attrs, err := objects.Next()
		if err == iterator.Done {
			return names
		}
		require.NoError(t, err)
		names = append(names, attrs.Name)
	}
}
