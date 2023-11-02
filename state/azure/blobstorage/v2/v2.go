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
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/azure/blobstorage/internal"
	"github.com/dapr/kit/logger"
)

func getFileName(key string) string {
	// Nop in this version
	return key
}

func NewAzureBlobStorageStore(log logger.Logger) state.Store {
	return internal.NewAzureBlobStorageStore(log, getFileName)
}
