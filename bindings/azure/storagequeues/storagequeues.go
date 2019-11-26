// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package storagequeues

import "github.com/dapr/components-contrib/bindings"

// AzureStorageQueues is an input/output binding reading from and sending events to Azure Storage queues
type AzureStorageQueues struct {
}

type storageQueuesMetadata struct {
}

// NewAzureStorageQueues returns a new AzureStorageQueues instance
func NewAzureStorageQueues() *AzureStorageQueues {
	return &AzureStorageQueues{}
}

// Init parses connection properties and creates a new Storage Queue client
func (a *AzureStorageQueues) Init(metadata bindings.Metadata) error {
	return nil
}

func (a *AzureStorageQueues) parseMetadata(metadata bindings.Metadata) (*storageQueuesMetadata, error) {

	return nil, nil
}

func (a *AzureStorageQueues) Write(req *bindings.WriteRequest) error {
	return nil
}

func (a *AzureStorageQueues) Read(handler func(*bindings.ReadResponse) error) error {
	return nil
}
