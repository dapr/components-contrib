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
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"

	"github.com/dapr/components-contrib/bindings"
	azauth "github.com/dapr/components-contrib/common/authentication/azure"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
	"github.com/dapr/kit/ptr"
)

const (
	defaultTTL               = 10 * time.Minute
	defaultVisibilityTimeout = 30 * time.Second
	defaultPollingInterval   = 10 * time.Second
	dequeueCount             = "dequeueCount"
	insertionTime            = "insertionTime"
	expirationTime           = "expirationTime"
	nextVisibleTime          = "nextVisibleTime"
	popReceipt               = "popReceipt"
	messageID                = "messageID"
)

type consumer struct {
	callback bindings.Handler
}

// QueueHelper enables injection for testnig.
type QueueHelper interface {
	Init(ctx context.Context, metadata bindings.Metadata) (*storageQueuesMetadata, error)
	Write(ctx context.Context, data []byte, ttl *time.Duration) error
	Read(ctx context.Context, consumer *consumer) error
	Close() error
}

// AzureQueueHelper concrete impl of queue helper.
type AzureQueueHelper struct {
	queueClient       *azqueue.QueueClient
	logger            logger.Logger
	decodeBase64      bool
	encodeBase64      bool
	pollingInterval   time.Duration
	visibilityTimeout time.Duration
}

// Init sets up this helper.
func (d *AzureQueueHelper) Init(ctx context.Context, meta bindings.Metadata) (*storageQueuesMetadata, error) {
	m, err := parseMetadata(meta)
	if err != nil {
		return nil, err
	}

	azEnvSettings, err := azauth.NewEnvironmentSettings(meta.Properties)
	if err != nil {
		return nil, err
	}

	userAgent := "dapr-" + logger.DaprVersion
	options := azqueue.ClientOptions{
		ClientOptions: policy.ClientOptions{
			Telemetry: policy.TelemetryOptions{
				ApplicationID: userAgent,
			},
		},
	}

	var queueServiceClient *azqueue.ServiceClient
	if m.AccountKey != "" && m.AccountName != "" {
		var credential *azqueue.SharedKeyCredential
		credential, err = azqueue.NewSharedKeyCredential(m.AccountName, m.AccountKey)
		if err != nil {
			return nil, fmt.Errorf("invalid shared key credentials with error: %w", err)
		}
		queueServiceClient, err = azqueue.NewServiceClientWithSharedKeyCredential(m.GetQueueURL(azEnvSettings), credential, &options)
		if err != nil {
			return nil, fmt.Errorf("cannot init storage queue client with shared key: %w", err)
		}
	} else {
		credential, tokenErr := azEnvSettings.GetTokenCredential()
		if tokenErr != nil {
			return nil, fmt.Errorf("invalid token credentials with error: %w", tokenErr)
		}
		var clientErr error
		queueServiceClient, clientErr = azqueue.NewServiceClient(m.GetQueueURL(azEnvSettings), credential, &options)
		if clientErr != nil {
			return nil, fmt.Errorf("cannot init storage queue client with Azure AD token: %w", clientErr)
		}
	}

	d.decodeBase64 = m.DecodeBase64
	d.encodeBase64 = m.EncodeBase64
	d.pollingInterval = m.PollingInterval
	d.visibilityTimeout = *m.VisibilityTimeout
	d.queueClient = queueServiceClient.NewQueueClient(m.QueueName)

	createCtx, createCancel := context.WithTimeout(ctx, 2*time.Minute)
	_, err = d.queueClient.Create(createCtx, nil)
	createCancel()
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (d *AzureQueueHelper) Write(ctx context.Context, data []byte, ttl *time.Duration) error {
	var ttlSeconds *int32
	if ttl != nil {
		ttlSeconds = ptr.Of(int32(ttl.Seconds()))
	} else {
		ttlSeconds = ptr.Of(int32(defaultTTL.Seconds()))
	}

	s, err := strconv.Unquote(string(data))
	if err != nil {
		s = string(data)
	}

	if d.encodeBase64 {
		s = base64.StdEncoding.EncodeToString([]byte(s))
	}

	_, err = d.queueClient.EnqueueMessage(ctx, s, &azqueue.EnqueueMessageOptions{
		TimeToLive: ttlSeconds,
	})

	return err
}

func (d *AzureQueueHelper) Read(ctx context.Context, consumer *consumer) error {
	res, err := d.queueClient.DequeueMessages(ctx, &azqueue.DequeueMessagesOptions{
		NumberOfMessages:  ptr.Of(int32(1)),
		VisibilityTimeout: ptr.Of(int32(d.visibilityTimeout.Seconds())),
	})
	if err != nil {
		return err
	}
	if len(res.Messages) == 0 {
		// Queue was empty so back off seconds before trying again
		select {
		case <-time.After(d.pollingInterval):
		case <-ctx.Done():
		}
		return nil
	}
	mt := res.Messages[0].MessageText

	data := []byte("")
	if mt != nil {
		if d.decodeBase64 {
			decoded, decodeError := base64.StdEncoding.DecodeString(*mt)
			if decodeError != nil {
				return decodeError
			}
			data = decoded
		} else {
			data = []byte(*mt)
		}
	}

	metadata := make(map[string]string, 6)

	if res.Messages[0].MessageID != nil {
		metadata[messageID] = *res.Messages[0].MessageID
	}
	if res.Messages[0].PopReceipt != nil {
		metadata[popReceipt] = *res.Messages[0].PopReceipt
	}
	if res.Messages[0].InsertionTime != nil {
		metadata[insertionTime] = res.Messages[0].InsertionTime.Format(time.RFC3339)
	}
	if res.Messages[0].ExpirationTime != nil {
		metadata[expirationTime] = res.Messages[0].ExpirationTime.Format(time.RFC3339)
	}
	if res.Messages[0].TimeNextVisible != nil {
		metadata[nextVisibleTime] = res.Messages[0].TimeNextVisible.Format(time.RFC3339)
	}
	if res.Messages[0].DequeueCount != nil {
		metadata[dequeueCount] = strconv.FormatInt(*res.Messages[0].DequeueCount, 10)
	}

	_, err = consumer.callback(ctx, &bindings.ReadResponse{
		Data:     data,
		Metadata: metadata,
	})
	if err != nil {
		return err
	}

	if res.Messages[0].MessageID != nil && res.Messages[0].PopReceipt != nil {
		_, err = d.queueClient.DeleteMessage(ctx, *res.Messages[0].MessageID, *res.Messages[0].PopReceipt, nil)
		if err != nil {
			return err
		}
		return nil
	} else {
		return errors.New("could not delete message from queue: message ID or pop receipt is nil")
	}
}

func (d *AzureQueueHelper) Close() error {
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

	wg      sync.WaitGroup
	closeCh chan struct{}
	closed  atomic.Bool
}

type storageQueuesMetadata struct {
	QueueName         string
	QueueEndpoint     string
	AccountName       string
	AccountKey        string
	DecodeBase64      bool
	EncodeBase64      bool
	PollingInterval   time.Duration  `mapstructure:"pollingInterval"`
	TTL               *time.Duration `mapstructure:"ttl" mapstructurealiases:"ttlInSeconds"`
	VisibilityTimeout *time.Duration
}

func (m *storageQueuesMetadata) GetQueueURL(azEnvSettings azauth.EnvironmentSettings) string {
	var URL string
	if m.QueueEndpoint != "" {
		URL = fmt.Sprintf("%s/%s/", m.QueueEndpoint, m.AccountName)
	} else {
		URL = fmt.Sprintf("https://%s.queue.%s/", m.AccountName, azEnvSettings.EndpointSuffix(azauth.ServiceAzureStorage))
	}
	return URL
}

// NewAzureStorageQueues returns a new AzureStorageQueues instance.
func NewAzureStorageQueues(logger logger.Logger) bindings.InputOutputBinding {
	return &AzureStorageQueues{
		helper:  NewAzureQueueHelper(logger),
		logger:  logger,
		closeCh: make(chan struct{}),
	}
}

// Init parses connection properties and creates a new Storage Queue client.
func (a *AzureStorageQueues) Init(ctx context.Context, metadata bindings.Metadata) (err error) {
	a.metadata, err = a.helper.Init(ctx, metadata)
	if err != nil {
		return err
	}

	return nil
}

func parseMetadata(meta bindings.Metadata) (*storageQueuesMetadata, error) {
	m := storageQueuesMetadata{
		PollingInterval:   defaultPollingInterval,
		VisibilityTimeout: ptr.Of(defaultVisibilityTimeout),
	}
	err := kitmd.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return nil, fmt.Errorf("failed to decode metadata: %w", err)
	}

	if val, ok := contribMetadata.GetMetadataProperty(meta.Properties, azauth.MetadataKeys["StorageAccountName"]...); ok && val != "" {
		m.AccountName = val
	} else {
		return nil, fmt.Errorf("missing or empty %s field from metadata", azauth.MetadataKeys["StorageAccountName"][0])
	}

	if val, ok := contribMetadata.GetMetadataProperty(meta.Properties, azauth.MetadataKeys["StorageQueueName"]...); ok && val != "" {
		m.QueueName = val
	} else {
		return nil, fmt.Errorf("missing or empty %s field from metadata", azauth.MetadataKeys["StorageQueueName"][0])
	}

	if val, ok := contribMetadata.GetMetadataProperty(meta.Properties, azauth.MetadataKeys["StorageEndpoint"]...); ok && val != "" {
		m.QueueEndpoint = val
	}

	if val, ok := contribMetadata.GetMetadataProperty(meta.Properties, azauth.MetadataKeys["StorageAccountKey"]...); ok && val != "" {
		m.AccountKey = val
	}

	if m.PollingInterval < (100 * time.Millisecond) {
		return nil, errors.New("invalid value for 'pollingInterval': must be greater than 100ms")
	}

	ttl, ok, err := contribMetadata.TryGetTTL(meta.Properties)
	if err != nil {
		return nil, err
	}
	if ok {
		m.TTL = &ttl
	} else {
		m.TTL = nil
	}

	return &m, nil
}

func (a *AzureStorageQueues) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (a *AzureStorageQueues) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	ttlToUse := a.metadata.TTL
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
	if a.closed.Load() {
		return errors.New("input binding is closed")
	}

	c := consumer{
		callback: handler,
	}

	// Close read context when binding is closed.
	readCtx, cancel := context.WithCancel(ctx)
	a.wg.Add(2)
	go func() {
		defer a.wg.Done()
		defer cancel()

		select {
		case <-a.closeCh:
		case <-ctx.Done():
		}
	}()
	go func() {
		defer a.wg.Done()
		// Read until context is canceled
		var err error
		for readCtx.Err() == nil {
			err = a.helper.Read(readCtx, &c)
			if err != nil {
				a.logger.Errorf("error from c: %s", err)
			}
		}
	}()

	return nil
}

func (a *AzureStorageQueues) Close() error {
	if a.closed.CompareAndSwap(false, true) {
		close(a.closeCh)
	}
	a.wg.Wait()
	return nil
}

// GetComponentMetadata returns the metadata of the component.
func (a *AzureStorageQueues) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := storageQueuesMetadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.BindingType)
	return
}
