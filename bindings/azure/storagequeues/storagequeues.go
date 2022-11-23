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

package storagequeues

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/Azure/azure-storage-queue-go/azqueue"

	"github.com/dapr/components-contrib/bindings"
	azauth "github.com/dapr/components-contrib/internal/authentication/azure"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

const (
	defaultTTL = time.Minute * 10
)

type consumer struct {
	callback bindings.Handler
}

// QueueHelper enables injection for testnig.
type QueueHelper interface {
	Init(metadata bindings.Metadata) (*storageQueuesMetadata, error)
	Write(ctx context.Context, data []byte, ttl *time.Duration) error
	Read(ctx context.Context, consumer *consumer) error
}

// AzureQueueHelper concrete impl of queue helper.
type AzureQueueHelper struct {
	queueURL          azqueue.QueueURL
	logger            logger.Logger
	decodeBase64      bool
	encodeBase64      bool
	visibilityTimeout time.Duration
}

// Init sets up this helper.
func (d *AzureQueueHelper) Init(metadata bindings.Metadata) (*storageQueuesMetadata, error) {
	m, err := parseMetadata(metadata)
	if err != nil {
		return nil, err
	}

	credential, env, err := azauth.GetAzureStorageQueueCredentials(d.logger, m.AccountName, metadata.Properties)
	if err != nil {
		return nil, fmt.Errorf("invalid credentials with error: %s", err.Error())
	}

	userAgent := "dapr-" + logger.DaprVersion
	pipelineOptions := azqueue.PipelineOptions{
		Telemetry: azqueue.TelemetryOptions{
			Value: userAgent,
		},
	}
	p := azqueue.NewPipeline(credential, pipelineOptions)

	d.decodeBase64 = m.DecodeBase64
	d.encodeBase64 = m.EncodeBase64
	d.visibilityTimeout = *m.VisibilityTimeout

	if m.QueueEndpoint != "" {
		URL, parseErr := url.Parse(fmt.Sprintf("%s/%s/%s", m.QueueEndpoint, m.AccountName, m.QueueName))
		if parseErr != nil {
			return nil, parseErr
		}
		d.queueURL = azqueue.NewQueueURL(*URL, p)
	} else {
		URL, _ := url.Parse(fmt.Sprintf("https://%s.queue.%s/%s", m.AccountName, env.StorageEndpointSuffix, m.QueueName))
		d.queueURL = azqueue.NewQueueURL(*URL, p)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	_, err = d.queueURL.Create(ctx, azqueue.Metadata{})
	cancel()
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (d *AzureQueueHelper) Write(ctx context.Context, data []byte, ttl *time.Duration) error {
	messagesURL := d.queueURL.NewMessagesURL()

	s, err := strconv.Unquote(string(data))
	if err != nil {
		s = string(data)
	}

	if d.encodeBase64 {
		s = base64.StdEncoding.EncodeToString([]byte(s))
	}

	if ttl == nil {
		ttlToUse := defaultTTL
		ttl = &ttlToUse
	}
	_, err = messagesURL.Enqueue(ctx, s, time.Second*0, *ttl)

	return err
}

func (d *AzureQueueHelper) Read(ctx context.Context, consumer *consumer) error {
	messagesURL := d.queueURL.NewMessagesURL()
	res, err := messagesURL.Dequeue(ctx, 1, d.visibilityTimeout)
	if err != nil {
		return err
	}
	if res.NumMessages() == 0 {
		// Queue was empty so back off by 10 seconds before trying again
		time.Sleep(10 * time.Second)
		return nil
	}
	mt := res.Message(0).Text

	var data []byte

	if d.decodeBase64 {
		decoded, decodeError := base64.StdEncoding.DecodeString(mt)
		if decodeError != nil {
			return decodeError
		}
		data = decoded
	} else {
		data = []byte(mt)
	}

	_, err = consumer.callback(ctx, &bindings.ReadResponse{
		Data:     data,
		Metadata: map[string]string{},
	})
	if err != nil {
		return err
	}
	messageIDURL := messagesURL.NewMessageIDURL(res.Message(0).ID)
	pr := res.Message(0).PopReceipt
	_, err = messageIDURL.Delete(ctx, pr)
	if err != nil {
		return err
	}

	return nil
}

// NewAzureQueueHelper creates new helper.
func NewAzureQueueHelper(logger logger.Logger) QueueHelper {
	return &AzureQueueHelper{
		logger: logger,
	}
}

// AzureStorageQueues is an input/output binding reading from and sending events to Azure Storage queues.
type AzureStorageQueues struct {
	metadata *storageQueuesMetadata
	helper   QueueHelper

	logger logger.Logger
}

type storageQueuesMetadata struct {
	QueueName         string
	QueueEndpoint     string
	AccountName       string
	DecodeBase64      bool
	EncodeBase64      bool
	ttl               *time.Duration
	VisibilityTimeout *time.Duration
}

// NewAzureStorageQueues returns a new AzureStorageQueues instance.
func NewAzureStorageQueues(logger logger.Logger) bindings.InputOutputBinding {
	return &AzureStorageQueues{helper: NewAzureQueueHelper(logger), logger: logger}
}

// Init parses connection properties and creates a new Storage Queue client.
func (a *AzureStorageQueues) Init(metadata bindings.Metadata) (err error) {
	a.metadata, err = a.helper.Init(metadata)
	if err != nil {
		return err
	}

	return nil
}

func parseMetadata(meta bindings.Metadata) (*storageQueuesMetadata, error) {
	m := storageQueuesMetadata{
		VisibilityTimeout: ptr.Of(time.Second * 30),
	}
	// AccountKey is parsed in azauth

	contribMetadata.DecodeMetadata(meta.Properties, &m)

	if val, ok := contribMetadata.GetMetadataProperty(meta.Properties, azauth.StorageAccountNameKeys...); ok && val != "" {
		m.AccountName = val
	} else {
		return nil, fmt.Errorf("missing or empty %s field from metadata", azauth.StorageAccountNameKeys[0])
	}

	if val, ok := contribMetadata.GetMetadataProperty(meta.Properties, azauth.StorageQueueNameKeys...); ok && val != "" {
		m.QueueName = val
	} else {
		return nil, fmt.Errorf("missing or empty %s field from metadata", azauth.StorageQueueNameKeys[0])
	}

	if val, ok := contribMetadata.GetMetadataProperty(meta.Properties, azauth.StorageEndpointKeys...); ok && val != "" {
		m.QueueEndpoint = val
	}

	ttl, ok, err := contribMetadata.TryGetTTL(meta.Properties)
	if err != nil {
		return nil, err
	}
	if ok {
		m.ttl = &ttl
	}

	return &m, nil
}

func (a *AzureStorageQueues) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (a *AzureStorageQueues) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	ttlToUse := a.metadata.ttl
	ttl, ok, err := contribMetadata.TryGetTTL(req.Metadata)
	if err != nil {
		return nil, err
	}

	if ok {
		ttlToUse = &ttl
	}

	err = a.helper.Write(ctx, req.Data, ttlToUse)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (a *AzureStorageQueues) Read(ctx context.Context, handler bindings.Handler) error {
	c := consumer{
		callback: handler,
	}
	go func() {
		// Read until context is canceled
		var err error
		for ctx.Err() == nil {
			err = a.helper.Read(ctx, &c)
			if err != nil {
				a.logger.Errorf("error from c: %s", err)
			}
		}
	}()

	return nil
}
