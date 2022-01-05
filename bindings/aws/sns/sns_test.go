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
