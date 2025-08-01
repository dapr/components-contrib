/*
Copyright 2024 The Dapr Authors
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

package aws

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/secretsmanager"
	"github.com/aws/aws-sdk-go/service/secretsmanager/secretsmanageriface"
	"github.com/aws/aws-sdk-go/service/ssm"
	"github.com/aws/aws-sdk-go/service/ssm/ssmiface"
)

type MockParameterStore struct {
	GetParameterFn       func(context.Context, *ssm.GetParameterInput, ...request.Option) (*ssm.GetParameterOutput, error)
	DescribeParametersFn func(context.Context, *ssm.DescribeParametersInput, ...request.Option) (*ssm.DescribeParametersOutput, error)
	ssmiface.SSMAPI
}

func (m *MockParameterStore) GetParameterWithContext(ctx context.Context, input *ssm.GetParameterInput, option ...request.Option) (*ssm.GetParameterOutput, error) {
	return m.GetParameterFn(ctx, input, option...)
}

func (m *MockParameterStore) DescribeParametersWithContext(ctx context.Context, input *ssm.DescribeParametersInput, option ...request.Option) (*ssm.DescribeParametersOutput, error) {
	return m.DescribeParametersFn(ctx, input, option...)
}

type MockSecretManager struct {
	GetSecretValueFn func(context.Context, *secretsmanager.GetSecretValueInput, ...request.Option) (*secretsmanager.GetSecretValueOutput, error)
	secretsmanageriface.SecretsManagerAPI

	ListSecretsFn func(context.Context, *secretsmanager.ListSecretsInput, ...request.Option) (*secretsmanager.ListSecretsOutput, error)
}

func (m *MockSecretManager) GetSecretValueWithContext(ctx context.Context, input *secretsmanager.GetSecretValueInput, option ...request.Option) (*secretsmanager.GetSecretValueOutput, error) {
	return m.GetSecretValueFn(ctx, input, option...)
}

func (m *MockSecretManager) ListSecretsWithContext(ctx context.Context, input *secretsmanager.ListSecretsInput, option ...request.Option) (*secretsmanager.ListSecretsOutput, error) {
	return m.ListSecretsFn(ctx, input, option...)
}

type MockDynamoDB struct {
	GetItemWithContextFn            func(ctx context.Context, input *dynamodb.GetItemInput, op ...request.Option) (*dynamodb.GetItemOutput, error)
	PutItemWithContextFn            func(ctx context.Context, input *dynamodb.PutItemInput, op ...request.Option) (*dynamodb.PutItemOutput, error)
	DeleteItemWithContextFn         func(ctx context.Context, input *dynamodb.DeleteItemInput, op ...request.Option) (*dynamodb.DeleteItemOutput, error)
	BatchWriteItemWithContextFn     func(ctx context.Context, input *dynamodb.BatchWriteItemInput, op ...request.Option) (*dynamodb.BatchWriteItemOutput, error)
	TransactWriteItemsWithContextFn func(aws.Context, *dynamodb.TransactWriteItemsInput, ...request.Option) (*dynamodb.TransactWriteItemsOutput, error)
	dynamodbiface.DynamoDBAPI
}

func (m *MockDynamoDB) GetItemWithContext(ctx context.Context, input *dynamodb.GetItemInput, op ...request.Option) (*dynamodb.GetItemOutput, error) {
	return m.GetItemWithContextFn(ctx, input, op...)
}

func (m *MockDynamoDB) PutItemWithContext(ctx context.Context, input *dynamodb.PutItemInput, op ...request.Option) (*dynamodb.PutItemOutput, error) {
	return m.PutItemWithContextFn(ctx, input, op...)
}

func (m *MockDynamoDB) DeleteItemWithContext(ctx context.Context, input *dynamodb.DeleteItemInput, op ...request.Option) (*dynamodb.DeleteItemOutput, error) {
	return m.DeleteItemWithContextFn(ctx, input, op...)
}

func (m *MockDynamoDB) BatchWriteItemWithContext(ctx context.Context, input *dynamodb.BatchWriteItemInput, op ...request.Option) (*dynamodb.BatchWriteItemOutput, error) {
	return m.BatchWriteItemWithContextFn(ctx, input, op...)
}

func (m *MockDynamoDB) TransactWriteItemsWithContext(ctx context.Context, input *dynamodb.TransactWriteItemsInput, op ...request.Option) (*dynamodb.TransactWriteItemsOutput, error) {
	return m.TransactWriteItemsWithContextFn(ctx, input, op...)
}
