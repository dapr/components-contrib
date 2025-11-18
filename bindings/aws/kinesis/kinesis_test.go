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

package kinesis

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"accessKey":       "key",
		"region":          "region",
		"secretKey":       "secret",
		"consumerName":    "test",
		"streamName":      "stream",
		"mode":            "extended",
		"endpoint":        "endpoint",
		"sessionToken":    "token",
		"applicationName": "applicationName",
	}
	kinesis := AWSKinesis{}
	meta, err := kinesis.parseMetadata(m)
	require.NoError(t, err)
	assert.Equal(t, "key", meta.AccessKey)
	assert.Equal(t, "region", meta.Region)
	assert.Equal(t, "secret", meta.SecretKey)
	assert.Equal(t, "test", meta.ConsumerName)
	assert.Equal(t, "stream", meta.StreamName)
	assert.Equal(t, "endpoint", meta.Endpoint)
	assert.Equal(t, "token", meta.SessionToken)
	assert.Equal(t, "extended", meta.KinesisConsumerMode)
	assert.Equal(t, "applicationName", meta.ApplicationName)
}

func getStreamARN(ctx context.Context, client *kinesis.Client, streamName string) (*string, error) {
	if client == nil {
		return nil, errors.New("unable to get stream arn due to empty client")
	}
	stream, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamName: &streamName,
	})
	if err != nil {
		return nil, err
	}
	return stream.StreamDescription.StreamARN, nil
}

func TestKinesisClient_Stream(t *testing.T) {
	tests := []struct {
		name          string
		kinesisClient *kinesis.Client
		streamName    string
		expectedErr   string
	}{
		{
			name:          "returns error when client is nil",
			kinesisClient: nil,
			streamName:    "test-stream",
			expectedErr:   "unable to get stream arn due to empty client",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			got, err := getStreamARN(ctx, tt.kinesisClient, tt.streamName)

			if tt.expectedErr != "" {
				require.Error(t, err)
				assert.Equal(t, tt.expectedErr, err.Error())
				assert.Nil(t, got)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, got)
			}
		})
	}
}

func TestAWSKinesis_WorkerCfg(t *testing.T) {
	tests := []struct {
		name            string
		streamName      string
		applicationName string
		mode            string
		expectNil       bool
	}{
		{
			name:            "returns config for shared mode",
			streamName:      "test-stream",
			applicationName: "test-app",
			mode:            "shared",
			expectNil:       false,
		},
		{
			name:            "returns nil for extended mode",
			streamName:      "test-stream",
			applicationName: "test-app",
			mode:            "extended",
			expectNil:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			awsKinesis := &AWSKinesis{
				v2Credentials: aws.NewCredentialsCache(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
					return aws.Credentials{
						AccessKeyID:     "test",
						SecretAccessKey: "test",
					}, nil
				})),
			}
			cfg := awsKinesis.workerCfg(ctx, tt.streamName, "us-west-2", tt.mode, tt.applicationName)

			if tt.expectNil {
				assert.Nil(t, cfg)
			} else {
				assert.NotNil(t, cfg)
				if cfg != nil {
					assert.Equal(t, tt.streamName, cfg.StreamName)
					assert.Equal(t, tt.applicationName, cfg.ApplicationName)
					assert.Equal(t, "us-west-2", cfg.RegionName)
				}
			}
		})
	}
}
