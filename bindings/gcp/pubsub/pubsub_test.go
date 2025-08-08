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

package pubsub

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
)

func TestInit(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"authProviderX509CertURL": "https://auth",
		"authURI":                 "https://auth",
		"clientX509CertURL":       "https://cert",
		"clientEmail":             "test@test.com",
		"clientID":                "id",
		"privateKey":              "****",
		"privateKeyID":            "key_id",
		"projectID":               "project1",
		"tokenURI":                "https://token",
		"type":                    "serviceaccount",
		"topic":                   "t1",
		"subscription":            "s1",
	}

	// Test metadata parsing only
	pubsubMeta, err := parseMetadata(m)
	require.NoError(t, err)

	assert.Equal(t, "s1", pubsubMeta.Subscription)
	assert.Equal(t, "t1", pubsubMeta.Topic)
	assert.Equal(t, "https://auth", pubsubMeta.AuthProviderX509CertURL)
	assert.Equal(t, "https://auth", pubsubMeta.AuthURI)
	assert.Equal(t, "https://cert", pubsubMeta.ClientX509CertURL)
	assert.Equal(t, "test@test.com", pubsubMeta.ClientEmail)
	assert.Equal(t, "id", pubsubMeta.ClientID)
	assert.Equal(t, "****", pubsubMeta.PrivateKey)
	assert.Equal(t, "key_id", pubsubMeta.PrivateKeyID)
	assert.Equal(t, "project1", pubsubMeta.ProjectID)
	assert.Equal(t, "https://token", pubsubMeta.TokenURI)
	assert.Equal(t, "serviceaccount", pubsubMeta.Type)
}
