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
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmware/vmware-go-kcl/clientlibrary/config"
)

type mockedSQS struct {
	sqsiface.SQSAPI
	GetQueueURLFn func(ctx context.Context, input *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error)
}

func (m *mockedSQS) GetQueueUrlWithContext(ctx context.Context, input *sqs.GetQueueUrlInput, opts ...request.Option) (*sqs.GetQueueUrlOutput, error) { //nolint:stylecheck
	return m.GetQueueURLFn(ctx, input)
}

type mockedKinesis struct {
	kinesisiface.KinesisAPI
	DescribeStreamFn func(ctx context.Context, input *kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error)
}

func (m *mockedKinesis) DescribeStreamWithContext(ctx context.Context, input *kinesis.DescribeStreamInput, opts ...request.Option) (*kinesis.DescribeStreamOutput, error) {
	return m.DescribeStreamFn(ctx, input)
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

			url, err := client.QueueURL(context.Background(), tt.queueName)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedURL, url)
			}
		})
	}
}

func TestKinesisClients_Stream(t *testing.T) {
	tests := []struct {
		name           string
		kinesisClient  *KinesisClients
		streamName     string
		mockStreamARN  *string
		mockError      error
		expectedStream *string
		expectedErr    error
	}{
		{
			name: "successfully retrieves stream ARN",
			kinesisClient: &KinesisClients{
				Kinesis: &mockedKinesis{DescribeStreamFn: func(ctx context.Context, input *kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {
					return &kinesis.DescribeStreamOutput{
						StreamDescription: &kinesis.StreamDescription{
							StreamARN: aws.String("arn:aws:kinesis:some-region:123456789012:stream/some-stream"),
						},
					}, nil
				}},
				Region:      "us-west-1",
				Credentials: credentials.NewStaticCredentials("accessKey", "secretKey", ""),
			},
			streamName:     "some-stream",
			expectedStream: aws.String("arn:aws:kinesis:some-region:123456789012:stream/some-stream"),
			expectedErr:    nil,
		},
		{
			name: "returns error when stream not found",
			kinesisClient: &KinesisClients{
				Kinesis: &mockedKinesis{DescribeStreamFn: func(ctx context.Context, input *kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {
					return nil, errors.New("stream not found")
				}},
				Region:      "us-west-1",
				Credentials: credentials.NewStaticCredentials("accessKey", "secretKey", ""),
			},
			streamName:     "nonexistent-stream",
			expectedStream: nil,
			expectedErr:    errors.New("unable to get stream arn due to empty client"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.kinesisClient.Stream(context.Background(), tt.streamName)
			if tt.expectedErr != nil {
				require.Error(t, err)
				assert.Equal(t, tt.expectedErr.Error(), err.Error())
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedStream, got)
			}
		})
	}
}

func TestKinesisClients_WorkerCfg(t *testing.T) {
	testCreds := credentials.NewStaticCredentials("accessKey", "secretKey", "")
	tests := []struct {
		name           string
		kinesisClient  *KinesisClients
		streamName     string
		consumer       string
		mode           string
		expectedConfig *config.KinesisClientLibConfiguration
	}{
		{
			name: "successfully creates shared mode worker config",
			kinesisClient: &KinesisClients{
				Kinesis: &mockedKinesis{
					DescribeStreamFn: func(ctx context.Context, input *kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {
						return &kinesis.DescribeStreamOutput{
							StreamDescription: &kinesis.StreamDescription{
								StreamARN: aws.String("arn:aws:kinesis:us-east-1:123456789012:stream/existing-stream"),
							},
						}, nil
					},
				},
				Region:      "us-west-1",
				Credentials: testCreds,
			},
			streamName: "existing-stream",
			consumer:   "consumer1",
			mode:       "shared",
			expectedConfig: config.NewKinesisClientLibConfigWithCredential(
				"consumer1", "existing-stream", "us-west-1", "consumer1", testCreds,
			),
		},
		{
			name: "returns nil when mode is not shared",
			kinesisClient: &KinesisClients{
				Kinesis: &mockedKinesis{
					DescribeStreamFn: func(ctx context.Context, input *kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {
						return &kinesis.DescribeStreamOutput{
							StreamDescription: &kinesis.StreamDescription{
								StreamARN: aws.String("arn:aws:kinesis:us-east-1:123456789012:stream/existing-stream"),
							},
						}, nil
					},
				},
				Region:      "us-west-1",
				Credentials: testCreds,
			},
			streamName:     "existing-stream",
			consumer:       "consumer1",
			mode:           "exclusive",
			expectedConfig: nil,
		},
		{
			name: "returns nil when client is nil",
			kinesisClient: &KinesisClients{
				Kinesis:     nil,
				Region:      "us-west-1",
				Credentials: credentials.NewStaticCredentials("accessKey", "secretKey", ""),
			},
			streamName:     "existing-stream",
			consumer:       "consumer1",
			mode:           "shared",
			expectedConfig: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.kinesisClient.WorkerCfg(context.Background(), tt.streamName, tt.consumer, tt.mode)
			if tt.expectedConfig == nil {
				assert.Equal(t, tt.expectedConfig, cfg)
				return
			}
			assert.Equal(t, tt.expectedConfig.StreamName, cfg.StreamName)
			assert.Equal(t, tt.expectedConfig.EnhancedFanOutConsumerName, cfg.EnhancedFanOutConsumerName)
			assert.Equal(t, tt.expectedConfig.EnableEnhancedFanOutConsumer, cfg.EnableEnhancedFanOutConsumer)
			assert.Equal(t, tt.expectedConfig.RegionName, cfg.RegionName)
		})
	}
}
