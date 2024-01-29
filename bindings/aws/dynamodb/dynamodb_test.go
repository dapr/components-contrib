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

package dynamodb

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
)

type mockedDynamoDB struct {
	GetItemWithContextFn func(ctx context.Context, input *dynamodb.GetItemInput, op ...request.Option) (*dynamodb.GetItemOutput, error)
	dynamodb.DynamoDB
}

func (m *mockedDynamoDB) GetItemWithContext(ctx context.Context, input *dynamodb.GetItemInput, op ...request.Option) (*dynamodb.GetItemOutput, error) {
	return m.GetItemWithContextFn(ctx, input, op...)
}

func TestInit(t *testing.T) {
	b := &DynamoDB{
		table:  "test-table",
		client: &mockedDynamoDB{},
	}

	t.Run("Init with bad table name or permissions", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"Table":  "does-not-exist",
			"Region": "eu-west-1",
		}

		b.client = &mockedDynamoDB{
			GetItemWithContextFn: func(ctx context.Context, input *dynamodb.GetItemInput, op ...request.Option) (*dynamodb.GetItemOutput, error) {
				return nil, errors.New("Requested resource not found")
			},
		}

		err := b.Init(context.Background(), m)
		require.Error(t, err)
		require.EqualError(t, err, "error validating DynamoDB table 'does-not-exist' access: Requested resource not found")
	})

}

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"AccessKey": "a", "Region": "a", "SecretKey": "a", "Table": "a", "Endpoint": "a", "SessionToken": "t",
	}
	dy := DynamoDB{}
	meta, err := dy.getDynamoDBMetadata(m)
	require.NoError(t, err)
	assert.Equal(t, "a", meta.AccessKey)
	assert.Equal(t, "a", meta.Region)
	assert.Equal(t, "a", meta.SecretKey)
	assert.Equal(t, "a", meta.Table)
	assert.Equal(t, "a", meta.Endpoint)
	assert.Equal(t, "t", meta.SessionToken)
}
