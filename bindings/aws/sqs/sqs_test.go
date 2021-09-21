// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package sqs

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"QueueName": "a", "Region": "a", "AccessKey": "a", "SecretKey": "a", "Endpoint": "a", "SessionToken": "t",
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
}
