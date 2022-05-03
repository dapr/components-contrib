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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"QueueName": "a", "Region": "a", "AccessKey": "a", "SecretKey": "a", "Endpoint": "a", "SessionToken": "t", "QueueURL": "a",
	}
	s := AWSSQS{}
	sqsM, err := s.parseSQSMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "a", sqsM.QueueName)
	assert.Equal(t, "a", sqsM.Region)
	assert.Equal(t, "a", sqsM.AccessKey)
	assert.Equal(t, "a", sqsM.SecretKey)
	assert.Equal(t, "a", sqsM.Endpoint)
	assert.Equal(t, "t", sqsM.SessionToken)
	assert.Equal(t, "a", sqsM.QueueURL)
}
