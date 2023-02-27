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

package blobstorage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	azauth "github.com/dapr/components-contrib/internal/authentication/azure"
	"github.com/dapr/kit/logger"
)

type scenario struct {
	metadata                 map[string]string
	expectedFailureSubString string
}

func TestClientInitFailures(t *testing.T) {
	log := logger.NewLogger("test")

	scenarios := map[string]scenario{
		"missing accountName": {
			metadata:                 createTestMetadata(false, true, true),
			expectedFailureSubString: "missing or empty accountName field from metadata",
		},
		"missing container": {
			metadata:                 createTestMetadata(true, true, false),
			expectedFailureSubString: "missing or empty containerName field from metadata",
		},
	}

	for name, s := range scenarios {
		t.Run(name, func(t *testing.T) {
			_, _, err := CreateContainerStorageClient(context.Background(), log, s.metadata)
			assert.Contains(t, err.Error(), s.expectedFailureSubString)
		})
	}
}

func createTestMetadata(accountName bool, accountKey bool, container bool) map[string]string {
	m := map[string]string{}
	if accountName {
		m[azauth.MetadataKeys["StorageAccountName"][0]] = "account"
	}
	if accountKey {
		m[azauth.MetadataKeys["StorageAccountKey"][0]] = "key"
	}
	if container {
		m[azauth.MetadataKeys["StorageContainerName"][0]] = "test"
	}
	return m
}
