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
	metadata *storageQueuesMetadata
	queueURL azqueue.QueueURL
}

type storageQueuesMetadata struct {
	AccountKey  string `json:"accountKey"`
	QueueName   string `json:"queueName"`
	AccountName string `json:"accountName"`
}

// NewAzureStorageQueues returns a new AzureStorageQueues instance
func NewAzureStorageQueues() *AzureStorageQueues {
	return &AzureStorageQueues{}
}

// Init parses connection properties and creates a new Storage Queue client
func (a *AzureStorageQueues) Init(metadata bindings.Metadata) error {
	meta, err := a.parseMetadata(metadata)
	if err != nil {
		return err
	}
	a.metadata = meta

	u, _ := url.Parse(fmt.Sprintf("https://%s.queue.core.windows.net/%s", a.metadata.AccountName, a.metadata.QueueName))

	credential, err := azqueue.NewSharedKeyCredential(a.metadata.AccountName, a.metadata.AccountKey)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.TODO()
	a.queueURL = azqueue.NewQueueURL(*u, azqueue.NewPipeline(credential, azqueue.PipelineOptions{}))
	_, err = a.queueURL.Create(ctx, azqueue.Metadata{})
	if err != nil {
		log.Fatal(err)
	}

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

	ctx := context.TODO()
	messagesURL := a.queueURL.NewMessagesURL()
	s := string(req.Data[:])
	_, err := messagesURL.Enqueue(ctx, s, time.Second*0, time.Minute*10)
	if err != nil {
		log.Fatal(err)
	}
	return nil
}

func (a *AzureStorageQueues) Read(handler func(*bindings.ReadResponse) error) error {
	return nil
}
