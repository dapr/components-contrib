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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/dapr/components-contrib/bindings"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"accessKey":    "key",
		"region":       "region",
		"secretKey":    "secret",
		"consumerName": "test",
		"streamName":   "stream",
		"mode":         "extended",
		"endpoint":     "endpoint",
		"sessionToken": "token",
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
}

func TestReadConfigError(t *testing.T) {
	kinesis := &AWSKinesis{
		metadata:     &kinesisMetadata{KinesisConsumerMode: SharedThroughput},
		awsCfg:       aws.Config{},
		streamName:   "stream",
		consumerName: "consumer",
		consumerMode: SharedThroughput,
	}
	err := kinesis.Read(t.Context(), func(ctx context.Context, resp *bindings.ReadResponse) ([]byte, error) {
		return nil, nil
	})
	require.Error(t, err)
	errStr := err.Error()
	assert.Contains(t, errStr, "unable to build kinesis worker configuration")
	assert.Contains(t, errStr, "region is required for Kinesis worker config")
}
