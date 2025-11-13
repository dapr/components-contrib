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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/aws-sdk-go/aws/credentials"
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

type mockedKinesisV2 struct {
	DescribeStreamFn func(ctx context.Context, input *kinesis.DescribeStreamInput, opts ...func(*kinesis.Options)) (*kinesis.DescribeStreamOutput, error)
}

func (m *mockedKinesisV2) DescribeStream(ctx context.Context, input *kinesis.DescribeStreamInput, opts ...func(*kinesis.Options)) (*kinesis.DescribeStreamOutput, error) {
	return m.DescribeStreamFn(ctx, input, opts...)
}

// testKinesisClients wraps KinesisClients for testing with mock
type testKinesisClients struct {
	*KinesisClients
	mockKinesis *mockedKinesisV2
}

func (t *testKinesisClients) Stream(ctx context.Context, streamName string) (*string, error) {
	if t.mockKinesis != nil {
		stream, err := t.mockKinesis.DescribeStream(ctx, &kinesis.DescribeStreamInput{
			StreamName: &streamName,
		})
		if err != nil {
			return nil, err
		}
		return stream.StreamDescription.StreamARN, nil
	}
	return nil, errors.New("unable to get stream arn due to empty client")
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

func TestKinesisClients_Stream(t *testing.T) {
	tests := []struct {
		name           string
		kinesisClient  *testKinesisClients
		streamName     string
		expectedStream *string
		expectedErr    string
	}{
		{
			name: "successfully retrieves stream ARN",
			kinesisClient: &testKinesisClients{
				KinesisClients: &KinesisClients{
					Region:      "us-west-2",
					Credentials: credentials.NewStaticCredentials("accessKey", "secretKey", ""),
				},
				mockKinesis: &mockedKinesisV2{
					DescribeStreamFn: func(ctx context.Context, input *kinesis.DescribeStreamInput, opts ...func(*kinesis.Options)) (*kinesis.DescribeStreamOutput, error) {
						streamARN := "arn:aws:kinesis:us-west-2:123456789012:stream/test-stream"
						streamName := "test-stream"
						return &kinesis.DescribeStreamOutput{
							StreamDescription: &types.StreamDescription{
								StreamARN:    &streamARN,
								StreamName:   &streamName,
								StreamStatus: types.StreamStatusActive,
							},
						}, nil
					},
				},
			},
			streamName:     "test-stream",
			expectedStream: aws.String("arn:aws:kinesis:us-west-2:123456789012:stream/test-stream"),
			expectedErr:    "",
		},
		{
			name: "returns error when stream not found",
			kinesisClient: &testKinesisClients{
				KinesisClients: &KinesisClients{
					Region:      "us-west-2",
					Credentials: credentials.NewStaticCredentials("accessKey", "secretKey", ""),
				},
				mockKinesis: &mockedKinesisV2{
					DescribeStreamFn: func(ctx context.Context, input *kinesis.DescribeStreamInput, opts ...func(*kinesis.Options)) (*kinesis.DescribeStreamOutput, error) {
						return nil, errors.New("ResourceNotFoundException: Stream nonexistent-stream under account 123456789012 not found")
					},
				},
			},
			streamName:     "nonexistent-stream",
			expectedStream: nil,
			expectedErr:    "ResourceNotFoundException: Stream nonexistent-stream under account 123456789012 not found",
		},
		{
			name: "returns error when client is nil",
			kinesisClient: &testKinesisClients{
				KinesisClients: &KinesisClients{
					Region:      "us-west-2",
					Credentials: credentials.NewStaticCredentials("accessKey", "secretKey", ""),
				},
				mockKinesis: nil,
			},
			streamName:     "test-stream",
			expectedStream: nil,
			expectedErr:    "unable to get stream arn due to empty client",
		},
		{
			name: "handles stream with special characters in name",
			kinesisClient: &testKinesisClients{
				KinesisClients: &KinesisClients{
					Region:      "eu-central-1",
					Credentials: credentials.NewStaticCredentials("accessKey", "secretKey", ""),
				},
				mockKinesis: &mockedKinesisV2{
					DescribeStreamFn: func(ctx context.Context, input *kinesis.DescribeStreamInput, opts ...func(*kinesis.Options)) (*kinesis.DescribeStreamOutput, error) {
						streamARN := "arn:aws:kinesis:eu-central-1:123456789012:stream/my-test_stream.123"
						streamName := "my-test_stream.123"
						return &kinesis.DescribeStreamOutput{
							StreamDescription: &types.StreamDescription{
								StreamARN:    &streamARN,
								StreamName:   &streamName,
								StreamStatus: types.StreamStatusActive,
							},
						}, nil
					},
				},
			},
			streamName:     "my-test_stream.123",
			expectedStream: aws.String("arn:aws:kinesis:eu-central-1:123456789012:stream/my-test_stream.123"),
			expectedErr:    "",
		},
		{
			name: "handles empty stream name",
			kinesisClient: &testKinesisClients{
				KinesisClients: &KinesisClients{
					Region:      "us-east-1",
					Credentials: credentials.NewStaticCredentials("accessKey", "secretKey", ""),
				},
				mockKinesis: &mockedKinesisV2{
					DescribeStreamFn: func(ctx context.Context, input *kinesis.DescribeStreamInput, opts ...func(*kinesis.Options)) (*kinesis.DescribeStreamOutput, error) {
						return nil, errors.New("ValidationException: Stream name cannot be empty")
					},
				},
			},
			streamName:     "",
			expectedStream: nil,
			expectedErr:    "ValidationException: Stream name cannot be empty",
		},
		{
			name: "handles stream in creating state",
			kinesisClient: &testKinesisClients{
				KinesisClients: &KinesisClients{
					Region:      "ap-southeast-1",
					Credentials: credentials.NewStaticCredentials("accessKey", "secretKey", ""),
				},
				mockKinesis: &mockedKinesisV2{
					DescribeStreamFn: func(ctx context.Context, input *kinesis.DescribeStreamInput, opts ...func(*kinesis.Options)) (*kinesis.DescribeStreamOutput, error) {
						streamARN := "arn:aws:kinesis:ap-southeast-1:123456789012:stream/creating-stream"
						streamName := "creating-stream"
						return &kinesis.DescribeStreamOutput{
							StreamDescription: &types.StreamDescription{
								StreamARN:    &streamARN,
								StreamName:   &streamName,
								StreamStatus: types.StreamStatusCreating,
							},
						}, nil
					},
				},
			},
			streamName:     "creating-stream",
			expectedStream: aws.String("arn:aws:kinesis:ap-southeast-1:123456789012:stream/creating-stream"),
			expectedErr:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			got, err := tt.kinesisClient.Stream(ctx, tt.streamName)

			if tt.expectedErr != "" {
				require.Error(t, err)
				assert.Equal(t, tt.expectedErr, err.Error())
				assert.Nil(t, got)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedStream, got)
			}
		})
	}
}

func TestKinesisClients_WorkerCfg(t *testing.T) {
	tests := []struct {
		name            string
		kinesisClient   *KinesisClients
		streamName      string
		applicationName string
		mode            string
		expectNil       bool
	}{
		{
			name: "successfully creates shared mode worker config with v2 credentials",
			kinesisClient: &KinesisClients{
				Region:      "us-west-2",
				Credentials: credentials.NewStaticCredentials("accessKey", "secretKey", ""),
				V2Credentials: aws.NewCredentialsCache(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
					return aws.Credentials{
						AccessKeyID:     "accessKey",
						SecretAccessKey: "secretKey",
					}, nil
				})),
			},
			streamName:      "test-stream",
			applicationName: "test-app",
			mode:            "shared",
			expectNil:       false,
		},
		{
			name: "returns nil when mode is not shared",
			kinesisClient: &KinesisClients{
				Region:      "us-west-2",
				Credentials: credentials.NewStaticCredentials("accessKey", "secretKey", ""),
			},
			streamName:      "test-stream",
			applicationName: "test-app",
			mode:            "extended",
			expectNil:       true,
		},
		{
			name: "falls back to default config when v2 credentials are nil",
			kinesisClient: &KinesisClients{
				Region:        "us-east-1",
				Credentials:   credentials.NewStaticCredentials("accessKey", "secretKey", ""),
				V2Credentials: nil,
			},
			streamName:      "fallback-stream",
			applicationName: "fallback-app",
			mode:            "shared",
			expectNil:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			cfg := tt.kinesisClient.WorkerCfg(ctx, tt.streamName, tt.kinesisClient.Region, tt.mode, tt.applicationName)

			if tt.expectNil {
				assert.Nil(t, cfg)
			} else {
				assert.NotNil(t, cfg)
				if cfg != nil {
					assert.Equal(t, tt.streamName, cfg.StreamName)
					assert.Equal(t, tt.applicationName, cfg.ApplicationName)
					assert.Equal(t, tt.kinesisClient.Region, cfg.RegionName)
				}
			}
		})
	}
}
