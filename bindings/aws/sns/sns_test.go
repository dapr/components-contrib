// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package sns

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"TopicArn": "a", "Region": "a", "AccessKey": "a", "SecretKey": "a", "Endpoint": "a", "SessionToken": "t",
	}
	s := AWSSNS{}
	snsM, err := s.parseMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "a", snsM.TopicArn)
	assert.Equal(t, "a", snsM.Region)
	assert.Equal(t, "a", snsM.AccessKey)
	assert.Equal(t, "a", snsM.SecretKey)
	assert.Equal(t, "a", snsM.Endpoint)
	assert.Equal(t, "t", snsM.SessionToken)
}
