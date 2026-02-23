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

package sqs

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/dapr/components-contrib/bindings"
	awsCommon "github.com/dapr/components-contrib/common/aws"
	awsCommonAuth "github.com/dapr/components-contrib/common/aws/auth"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

// AWSSQS allows receiving and sending data to/from AWS SQS.
type AWSSQS struct {
	sqsClient *sqs.Client
	queueName string
	logger    logger.Logger
	wg        sync.WaitGroup
	closeCh   chan struct{}
	closed    atomic.Bool
}

// TODO: the metadata fields need updating to use the builtin aws auth provider fully and reflect in metadata.yaml
type sqsMetadata struct {
	QueueName    string `json:"queueName"`
	Region       string `json:"region"`
	Endpoint     string `json:"endpoint"`
	AccessKey    string `json:"accessKey"`
	SecretKey    string `json:"secretKey"`
	SessionToken string `json:"sessionToken"`
}

// NewAWSSQS returns a new AWS SQS instance.
func NewAWSSQS(logger logger.Logger) bindings.InputOutputBinding {
	return &AWSSQS{
		logger:  logger,
		closeCh: make(chan struct{}),
	}
}

// Init does metadata parsing and connection creation.
func (a *AWSSQS) Init(ctx context.Context, metadata bindings.Metadata) error {
	m, err := a.parseSQSMetadata(metadata)
	if err != nil {
		return err
	}

	configOpts := awsCommonAuth.Options{
		Logger:       a.logger,
		Properties:   metadata.Properties,
		Region:       m.Region,
		Endpoint:     m.Endpoint,
		AccessKey:    m.AccessKey,
		SecretKey:    m.SecretKey,
		SessionToken: m.SessionToken,
	}
	awsConfig, err := awsCommon.NewConfig(ctx, configOpts)
	if err != nil {
		return err
	}
	a.sqsClient = sqs.NewFromConfig(awsConfig)
	a.queueName = m.QueueName
	return nil
}

func (a *AWSSQS) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (a *AWSSQS) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	msgBody := string(req.Data)
	url, err := a.getQueueURL(ctx, a.queueName)
	if err != nil {
		a.logger.Errorf("failed to get queue url: %v", err)
		return nil, err
	}
	_, err = a.sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		MessageBody: &msgBody,
		QueueUrl:    url,
	})
	return nil, err
}

func (a *AWSSQS) Read(ctx context.Context, handler bindings.Handler) error {
	if a.closed.Load() {
		return errors.New("binding is closed")
	}
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		for {
			if ctx.Err() != nil || a.closed.Load() {
				return
			}
			url, err := a.getQueueURL(ctx, a.queueName)
			if err != nil {
				a.logger.Errorf("failed to get queue url: %v", err)
				continue
			}
			result, err := a.sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:              url,
				AttributeNames:        []sqsTypes.QueueAttributeName{"SentTimestamp"}, // Use string literal for attribute name
				MaxNumberOfMessages:   1,
				MessageAttributeNames: []string{"All"},
				WaitTimeSeconds:       20,
			})
			if err != nil {
				a.logger.Errorf("Unable to receive message from queue %q, %v.", url, err)
				continue
			}
			if len(result.Messages) > 0 {
				for _, m := range result.Messages {
					body := m.Body
					res := bindings.ReadResponse{
						Data: []byte(*body),
					}
					_, err := handler(ctx, &res)
					if err == nil {
						msgHandle := m.ReceiptHandle
						if msgHandle != nil {
							_, deleteError := a.sqsClient.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
								QueueUrl:      url,
								ReceiptHandle: msgHandle,
							})
							if deleteError != nil {
								a.logger.Errorf("failed to delete message from queue %q: %v", url, deleteError)
							}
						}
					}
				}
			}
			select {
			case <-ctx.Done():
			case <-a.closeCh:
			case <-time.After(time.Millisecond * 50):
			}
		}
	}()
	return nil
}

func (a *AWSSQS) getQueueURL(ctx context.Context, queueName string) (*string, error) {
	out, err := a.sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	})
	if err != nil {
		return nil, err
	}
	return out.QueueUrl, nil
}

func (a *AWSSQS) Close() error {
	if a.closed.CompareAndSwap(false, true) {
		close(a.closeCh)
	}
	a.wg.Wait()
	return nil
}

func (a *AWSSQS) parseSQSMetadata(meta bindings.Metadata) (*sqsMetadata, error) {
	m := sqsMetadata{}
	err := kitmd.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

// GetComponentMetadata returns the metadata of the component.
func (a *AWSSQS) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := sqsMetadata{}
	if err := metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType); err != nil {
		if a != nil && a.logger != nil {
			a.logger.Errorf("failed to get component metadata: %v", err)
		}
	}
	return
}
