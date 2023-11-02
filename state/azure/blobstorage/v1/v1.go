/*
Copyright 2023 The Dapr Authors
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
	"strings"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/azure/blobstorage/internal"
	"github.com/dapr/kit/logger"
)

const (
	keyDelimiter = "||"
)

func getFileName(key string) string {
	// This function splits the prefix, such as the Dapr app ID, from the key.
	// This is wrong, as state stores should honor the prefix, but it is kept here for backwards-compatibility.
	// The v2 of the component fixes thie behavior.
	pr := strings.Split(key, keyDelimiter)
	if len(pr) != 2 {
		return pr[0]
	}

	return pr[1]
}

func NewAzureBlobStorageStore(log logger.Logger) state.Store {
	return internal.NewAzureBlobStorageStore(log, getFileName)
}
