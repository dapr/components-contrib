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
	"encoding/json"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/dapr/components-contrib/bindings"
	aws_auth "github.com/dapr/components-contrib/internal/authentication/aws"
	"github.com/dapr/kit/logger"
)

// AWSSQS allows receiving and sending data to/from AWS SQS.
type AWSSQS struct {
	Client   *sqs.SQS
	QueueURL *string

	logger logger.Logger
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
func NewAWSSQS(logger logger.Logger) *AWSSQS {
	return &AWSSQS{logger: logger}
}

// Init does metadata parsing and connection creation.
func (a *AWSSQS) Init(metadata bindings.Metadata) error {
	m, err := a.parseSQSMetadata(metadata)
	if err != nil {
		return err
	}

	client, err := a.getClient(m)
	if err != nil {
		return err
	}

	queueName := m.QueueName
	resultURL, err := client.GetQueueUrl(&sqs.GetQueueUrlInput{
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
	_, err := a.Client.SendMessage(&sqs.SendMessageInput{
		MessageBody: &msgBody,
		QueueUrl:    a.QueueURL,
	})

	return nil, err
}

func (a *AWSSQS) Read(handler bindings.Handler) error {
	for {
		result, err := a.Client.ReceiveMessage(&sqs.ReceiveMessageInput{
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
				_, err := handler(context.TODO(), &res)
				if err == nil {
					msgHandle := m.ReceiptHandle

					a.Client.DeleteMessage(&sqs.DeleteMessageInput{
						QueueUrl:      a.QueueURL,
						ReceiptHandle: msgHandle,
					})
				}
			}
		}

		time.Sleep(time.Millisecond * 50)
	}
}

func (a *AWSSQS) parseSQSMetadata(metadata bindings.Metadata) (*sqsMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var m sqsMetadata
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (a *AWSSQS) getClient(metadata *sqsMetadata) (*sqs.SQS, error) {
	sess, err := aws_auth.GetClient(metadata.AccessKey, metadata.SecretKey, metadata.SessionToken, metadata.Region, metadata.Endpoint)
	if err != nil {
		return nil, err
	}
	c := sqs.New(sess)

	return c, nil
}
