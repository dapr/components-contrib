// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package storagequeues

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/Azure/azure-storage-queue-go/azqueue"
	"github.com/dapr/components-contrib/bindings"
)

// AzureStorageQueues is an input/output binding reading from and sending events to Azure Storage queues
type AzureStorageQueues struct {
}

type storageQueuesMetadata struct {
	AccountKey string `json:"accountKey"`
	QueueName  string `json:"queueName"`
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
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var m storageQueuesMetadata
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func (a *AzureStorageQueues) Write(req *bindings.WriteRequest) error {
	var accountName = "daprcomp"
	var accountKey = "LDCIro1iN8LPqFo822X4Oecap/8s8anCNslB2pcxE1/pax/svHY7StnpGnOcIE1JiSU8IAVrag8t6QrjcYH/JQ=="

	u, _ := url.Parse(fmt.Sprintf("https://%s.queue.core.windows.net/queue6", accountName))

	credential, err := azqueue.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.TODO()
	queueURL := azqueue.NewQueueURL(*u, azqueue.NewPipeline(credential, azqueue.PipelineOptions{}))
	_, err = queueURL.Create(ctx, azqueue.Metadata{})
	if err != nil {
		log.Fatal(err)
	}

	messagesURL := queueURL.NewMessagesURL()

	_, err = messagesURL.Enqueue(ctx, "A message", time.Second*0, time.Minute*10)

	if err != nil {
		log.Fatal(err)
	}
	return nil
}

func (a *AzureStorageQueues) Read(handler func(*bindings.ReadResponse) error) error {
	return nil
}
