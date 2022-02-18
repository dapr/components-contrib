//go:build integration
// +build integration

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

package secretmanager

import (
	"os"
	"testing"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

// TestIntegrationGetSecret requires AWS specific environments for authentication AWS_DEFAULT_REGION AWS_ACCESS_KEY_ID,
// AWS_SECRET_ACCESS_KkEY and AWS_SESSION_TOKEN
func TestIntegrationGetSecret(t *testing.T) {
	secretName := "/aws/secret/testing"
	sm := NewSecretManager(logger.NewLogger("test"))
	err := sm.Init(secretstores.Metadata{
		Properties: map[string]string{
			"Region":       os.Getenv("AWS_DEFAULT_REGION"),
			"AccessKey":    os.Getenv("AWS_ACCESS_KEY_ID"),
			"SecretKey":    os.Getenv("AWS_SECRET_ACCESS_KEY"),
			"SessionToken": os.Getenv("AWS_SESSION_TOKEN"),
		},
	})
	assert.Nil(t, err)
	response, err := sm.GetSecret(secretstores.GetSecretRequest{
		Name:     secretName,
		Metadata: map[string]string{},
	})
	assert.Nil(t, err)
	assert.NotNil(t, response)
}

func TestIntegrationBulkGetSecret(t *testing.T) {
	secretName := "/aws/secret/testing"
	sm := NewSecretManager(logger.NewLogger("test"))
	err := sm.Init(secretstores.Metadata{
		Properties: map[string]string{
			"Region":       os.Getenv("AWS_DEFAULT_REGION"),
			"AccessKey":    os.Getenv("AWS_ACCESS_KEY_ID"),
			"SecretKey":    os.Getenv("AWS_SECRET_ACCESS_KEY"),
			"SessionToken": os.Getenv("AWS_SESSION_TOKEN"),
		},
	})
	assert.Nil(t, err)
	response, err := sm.BulkGetSecret(secretstores.BulkGetSecretRequest{})
	assert.Nil(t, err)
	assert.NotNil(t, response)
}
