/*
Copyright 2022 The Dapr Authors
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

package rocketmq

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/pubsub"
)

func TestMetaDataDecode(t *testing.T) {
	props := map[string]string{
		"accessProto":   "http",
		"accessKey":     "**",
		"secretKey":     "***",
		"nameServer":    "http://test.nameserver",
		"consumerGroup": "defaultGroup",
		"nameSpace":     "defaultNamespace",
	}
	pubsubMeta := pubsub.Metadata{Properties: props}
	metaData, err := parseRocketMQMetaData(pubsubMeta)
	require.NoError(t, err)
	assert.Equal(t, "http", metaData.AccessProto)
	assert.Equal(t, "**", metaData.AccessKey)
	assert.Equal(t, "***", metaData.SecretKey)
	assert.Equal(t, "defaultGroup", metaData.ConsumerGroup)
}
