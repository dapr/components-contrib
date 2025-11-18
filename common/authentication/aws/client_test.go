/*
Copyright 2024 The Dapr Authors
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

package aws

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockedSQS struct {
	sqsiface.SQSAPI
	GetQueueURLFn func(ctx context.Context, input *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error)
}

func (m *mockedSQS) GetQueueUrlWithContext(ctx context.Context, input *sqs.GetQueueUrlInput, opts ...request.Option) (*sqs.GetQueueUrlOutput, error) { //nolint:stylecheck
	return m.GetQueueURLFn(ctx, input)
}



func TestS3Clients_New(t *testing.T) {
	tests := []struct {
		name     string
		s3Client *S3Clients
		session  *session.Session
	}{
		{"initializes S3 client", &S3Clients{}, session.Must(session.NewSession())},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.s3Client.New(tt.session)
			require.NotNil(t, tt.s3Client.S3)
			require.NotNil(t, tt.s3Client.Uploader)
			require.NotNil(t, tt.s3Client.Downloader)
		})
	}
}

func TestSqsClients_QueueURL(t *testing.T) {
	tests := []struct {
		name        string
		mockFn      func() *mockedSQS
		queueName   string
		expectedURL *string
		expectError bool
	}{
		{
			name: "returns queue URL successfully",
			mockFn: func() *mockedSQS {
				return &mockedSQS{
					GetQueueURLFn: func(ctx context.Context, input *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
						return &sqs.GetQueueUrlOutput{
							QueueUrl: aws.String("https://sqs.aws.com/123456789012/queue"),
						}, nil
					},
				}
			},
			queueName:   "valid-queue",
			expectedURL: aws.String("https://sqs.aws.com/123456789012/queue"),
			expectError: false,
		},
		{
			name: "returns error when queue URL not found",
			mockFn: func() *mockedSQS {
				return &mockedSQS{
					GetQueueURLFn: func(ctx context.Context, input *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
						return nil, errors.New("unable to get stream arn due to empty client")
					},
				}
			},
			queueName:   "missing-queue",
			expectedURL: nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockSQS := tt.mockFn()

			// Initialize SqsClients with the mocked SQS client
			client := &SqsClients{
				Sqs: mockSQS,
			}

			url, err := client.QueueURL(t.Context(), tt.queueName)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedURL, url)
			}
		})
	}
}


