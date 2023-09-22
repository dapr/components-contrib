//go:build integration
// +build integration

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

package secretmanager

import (
	"context"
	"os"
	"testing"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

// TestIntegrationGetSecret requires either:
//   - Secret to exist with the key "/aws/secret/testing"
//   - AWS specific environments for authentication AWS_DEFAULT_REGION AWS_ACCESS_KEY_ID,
//     AWS_SECRET_ACCESS_KkEY and AWS_SESSION_TOKEN
//   - or Localstack https://docs.localstack.cloud/getting-started/quickstart/ with
//     AWS_ENDPOINT=http://localhost:4566
//     AWS_DEFAULT_REGION AWS_ACCESS_KEY_ID,
//     AWS_SECRET_ACCESS_KkEY and AWS_SESSION_TOKEN
func TestIntegrationGetSecret(t *testing.T) {
	secretName := "/aws/secret/testing"
	sm := NewSecretManager(logger.NewLogger("test"))
	m := secretstores.Metadata{}
	m.Properties = map[string]string{
		"Region":       os.Getenv("AWS_DEFAULT_REGION"),
		"AccessKey":    os.Getenv("AWS_ACCESS_KEY_ID"),
		"SecretKey":    os.Getenv("AWS_SECRET_ACCESS_KEY"),
		"SessionToken": os.Getenv("AWS_SESSION_TOKEN"),
		"Endpoint":     os.Getenv("AWS_ENDPOINT"),
	}
	err := sm.Init(context.Background(), m)
	assert.Nil(t, err)
	response, err := sm.GetSecret(context.Background(), secretstores.GetSecretRequest{
		Name:     secretName,
		Metadata: map[string]string{},
	})
	assert.Nil(t, err)
	assert.NotNil(t, response)
}

func TestIntegrationBulkGetSecret(t *testing.T) {
	sm := NewSecretManager(logger.NewLogger("test"))
	m := secretstores.Metadata{}
	m.Properties = map[string]string{
		"Region":       os.Getenv("AWS_DEFAULT_REGION"),
		"AccessKey":    os.Getenv("AWS_ACCESS_KEY_ID"),
		"SecretKey":    os.Getenv("AWS_SECRET_ACCESS_KEY"),
		"SessionToken": os.Getenv("AWS_SESSION_TOKEN"),
		"Endpoint":     os.Getenv("AWS_ENDPOINT"),
	}
	err := sm.Init(context.Background(), m)
	assert.Nil(t, err)
	response, err := sm.BulkGetSecret(context.Background(), secretstores.BulkGetSecretRequest{})
	assert.Nil(t, err)
	assert.NotNil(t, response)
}
