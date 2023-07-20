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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/dapr/components-contrib/bindings"
	awsAuth "github.com/dapr/components-contrib/internal/authentication/aws"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// AWSSQS allows receiving and sending data to/from AWS SQS.
type AWSSQS struct {
	Client   *sqs.SQS
	QueueURL *string

	logger  logger.Logger
	wg      sync.WaitGroup
	closeCh chan struct{}
	closed  atomic.Bool
}

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

	client, err := a.getClient(m)
	if err != nil {
		return err
	}

	queueName := m.QueueName
	resultURL, err := client.GetQueueUrlWithContext(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return err
	}

	a.QueueURL = resultURL.QueueUrl
	a.Client = client

	return nil
}

func (a *AWSSQS) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (a *AWSSQS) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	msgBody := string(req.Data)
	_, err := a.Client.SendMessageWithContext(ctx, &sqs.SendMessageInput{
		MessageBody: &msgBody,
		QueueUrl:    a.QueueURL,
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

		// Repeat until the context is canceled or component is closed
		for {
			if ctx.Err() != nil || a.closed.Load() {
				return
			}

			result, err := a.Client.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
				QueueUrl: a.QueueURL,
				AttributeNames: aws.StringSlice([]string{
					"SentTimestamp",
				}),
				MaxNumberOfMessages: aws.Int64(1),
				MessageAttributeNames: aws.StringSlice([]string{
					"All",
				}),
				WaitTimeSeconds: aws.Int64(20),
			})
			if err != nil {
				a.logger.Errorf("Unable to receive message from queue %q, %v.", *a.QueueURL, err)
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

						// Use a background context here because ctx may be canceled already
						a.Client.DeleteMessageWithContext(context.Background(), &sqs.DeleteMessageInput{
							QueueUrl:      a.QueueURL,
							ReceiptHandle: msgHandle,
						})
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

func (a *AWSSQS) Close() error {
	if a.closed.CompareAndSwap(false, true) {
		close(a.closeCh)
	}
	a.wg.Wait()
	return nil
}

func (a *AWSSQS) parseSQSMetadata(meta bindings.Metadata) (*sqsMetadata, error) {
	m := sqsMetadata{}
	err := metadata.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (a *AWSSQS) getClient(metadata *sqsMetadata) (*sqs.SQS, error) {
	sess, err := awsAuth.GetClient(metadata.AccessKey, metadata.SecretKey, metadata.SessionToken, metadata.Region, metadata.Endpoint)
	if err != nil {
		return nil, err
	}
	c := sqs.New(sess)

	return c, nil
}

// GetComponentMetadata returns the metadata of the component.
func (a *AWSSQS) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := sqsMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}
