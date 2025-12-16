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
	awsAuth "github.com/dapr/components-contrib/common/authentication/aws"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

// AWSSQS allows receiving and sending data to/from AWS SQS.
type AWSSQS struct {
	authProvider awsAuth.Provider
	queueName    string
	logger       logger.Logger
	wg           sync.WaitGroup
	closeCh      chan struct{}
	closed       atomic.Bool
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

	opts := awsAuth.Options{
		Logger:       a.logger,
		Properties:   metadata.Properties,
		Region:       m.Region,
		Endpoint:     m.Endpoint,
		AccessKey:    m.AccessKey,
		SecretKey:    m.SecretKey,
		SessionToken: m.SessionToken,
	}
	// extra configs needed per component type
	provider, err := awsAuth.NewProvider(ctx, opts, awsAuth.GetConfig(opts))
	if err != nil {
		return err
	}
	a.authProvider = provider
	a.queueName = m.QueueName

	return nil
}

func (a *AWSSQS) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (a *AWSSQS) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	msgBody := string(req.Data)
	url, err := a.authProvider.Sqs().QueueURL(ctx, a.queueName)
	if err != nil {
		a.logger.Errorf("failed to get queue url: %v", err)
	}

	_, err = a.authProvider.Sqs().Sqs.SendMessageWithContext(ctx, &sqs.SendMessageInput{
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

		// Repeat until the context is canceled or component is closed
		for {
			if ctx.Err() != nil || a.closed.Load() {
				return
			}
			url, err := a.authProvider.Sqs().QueueURL(ctx, a.queueName)
			if err != nil {
				a.logger.Errorf("failed to get queue url: %v", err)
			}

			result, err := a.authProvider.Sqs().Sqs.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
				QueueUrl: url,
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
				a.logger.Errorf("Unable to receive message from queue %q, %v.", url, err)
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
						a.authProvider.Sqs().Sqs.DeleteMessageWithContext(context.Background(), &sqs.DeleteMessageInput{
							QueueUrl:      url,
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
	if a.authProvider != nil {
		return a.authProvider.Close()
	}
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
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}
