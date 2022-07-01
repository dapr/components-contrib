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
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/Azure/azure-storage-queue-go/azqueue"

	azauth "github.com/dapr/components-contrib/authentication/azure"
	"github.com/dapr/components-contrib/bindings"
	contrib_metadata "github.com/dapr/components-contrib/metadata"
	mdutils "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const (
	defaultTTL = time.Minute * 10
)

type consumer struct {
	callback bindings.Handler
}

// QueueHelper enables injection for testnig.
type QueueHelper interface {
	Init(endpoint string, accountName string, accountKey string, queueName string, decodeBase64 bool) error
	Write(ctx context.Context, data []byte, ttl *time.Duration) error
	Read(ctx context.Context, consumer *consumer) error
}

// AzureQueueHelper concrete impl of queue helper.
type AzureQueueHelper struct {
	credential   *azqueue.SharedKeyCredential
	queueURL     azqueue.QueueURL
	reqURI       string
	logger       logger.Logger
	decodeBase64 bool
}

func getEndpoint(endpoint, reqURI, accountName, queueName string) (*url.URL, error) {
	if endpoint != "" {
		u, err := url.Parse(endpoint)
		if err != nil {
			return nil, err
		}

		p, err := url.Parse(queueName)
		if err != nil {
			return nil, err
		}

		return u.ResolveReference(p), nil
	}

	return url.Parse(fmt.Sprintf(reqURI, accountName, queueName))
}

// Init sets up this helper.
func (d *AzureQueueHelper) Init(endpoint string, accountName string, accountKey string, queueName string, decodeBase64 bool) error {
	credential, err := azqueue.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return err
	}
	d.credential = credential
	d.decodeBase64 = decodeBase64
	u, err := getEndpoint(endpoint, d.reqURI, accountName, queueName)
	if err != nil {
		return err
	}
	userAgent := "dapr-" + logger.DaprVersion
	pipelineOptions := azqueue.PipelineOptions{
		Telemetry: azqueue.TelemetryOptions{
			Value: userAgent,
		},
	}
	d.queueURL = azqueue.NewQueueURL(*u, azqueue.NewPipeline(credential, pipelineOptions))
	ctx := context.TODO()
	_, err = d.queueURL.Create(ctx, azqueue.Metadata{})
	if err != nil {
		return err
	}

	return nil
}

func (d *AzureQueueHelper) Write(ctx context.Context, data []byte, ttl *time.Duration) error {
	messagesURL := d.queueURL.NewMessagesURL()

	s, err := strconv.Unquote(string(data))
	if err != nil {
		s = string(data)
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
	res, err := messagesURL.Dequeue(ctx, 1, time.Second*30)
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
		reqURI: "https://%s.queue.core.windows.net/%s",
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
	AccountKey    string `json:"storageAccessKey"`
	QueueName     string `json:"queue"`
	QueueEndpoint string `json:"queueEndpointUrl"`
	AccountName   string `json:"storageAccount"`
	DecodeBase64  bool   `json:"decodeBase64"`
	ttl           *time.Duration
}

// NewAzureStorageQueues returns a new AzureStorageQueues instance.
func NewAzureStorageQueues(logger logger.Logger) *AzureStorageQueues {
	return &AzureStorageQueues{helper: NewAzureQueueHelper(logger), logger: logger}
}

// Init parses connection properties and creates a new Storage Queue client.
func (a *AzureStorageQueues) Init(metadata bindings.Metadata) error {
	meta, err := a.parseMetadata(metadata)
	if err != nil {
		return err
	}
	a.metadata = meta

	endpoint := ""
	if a.metadata.QueueEndpoint != "" {
		endpoint = a.metadata.QueueEndpoint
	}

	err = a.helper.Init(endpoint, a.metadata.AccountName, a.metadata.AccountKey, a.metadata.QueueName, a.metadata.DecodeBase64)
	if err != nil {
		return err
	}

	return nil
}

func (a *AzureStorageQueues) parseMetadata(metadata bindings.Metadata) (*storageQueuesMetadata, error) {
	var m storageQueuesMetadata
	// AccountKey is parsed in azauth

	if val, ok := mdutils.GetMetadataProperty(metadata.Properties, azauth.StorageAccountNameKeys...); ok && val != "" {
		m.AccountName = val
	} else {
		return nil, fmt.Errorf("missing or empty %s field from metadata", azauth.StorageAccountNameKeys[0])
	}

	if val, ok := mdutils.GetMetadataProperty(metadata.Properties, azauth.StorageQueueNameKeys...); ok && val != "" {
		m.QueueName = val
	} else {
		return nil, fmt.Errorf("missing or empty %s field from metadata", azauth.StorageQueueNameKeys[0])
	}

	if val, ok := mdutils.GetMetadataProperty(metadata.Properties, azauth.StorageEndpointKeys...); ok && val != "" {
		m.QueueEndpoint = val
	}

	m.DecodeBase64 = false
	if val, ok := metadata.Properties["decodeBase64"]; ok {
		n, err := strconv.ParseBool(val)
		if err != nil {
			return nil, fmt.Errorf("invalid decodeBase64 field from metadata")
		}
		m.DecodeBase64 = n
	}

	ttl, ok, err := contrib_metadata.TryGetTTL(metadata.Properties)
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
	ttl, ok, err := contrib_metadata.TryGetTTL(req.Metadata)
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

func (a *AzureStorageQueues) Read(handler bindings.Handler) error {
	c := consumer{
		callback: handler,
	}
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			err := a.helper.Read(ctx, &c)
			if err != nil {
				a.logger.Errorf("error from c: %s", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
	cancel()
	wg.Wait()

	return nil
}
