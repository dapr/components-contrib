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
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/state"
)

type mockedDynamoDB struct {
	GetItemWithContextFn            func(ctx context.Context, input *dynamodb.GetItemInput, op ...request.Option) (*dynamodb.GetItemOutput, error)
	PutItemWithContextFn            func(ctx context.Context, input *dynamodb.PutItemInput, op ...request.Option) (*dynamodb.PutItemOutput, error)
	DeleteItemWithContextFn         func(ctx context.Context, input *dynamodb.DeleteItemInput, op ...request.Option) (*dynamodb.DeleteItemOutput, error)
	BatchWriteItemWithContextFn     func(ctx context.Context, input *dynamodb.BatchWriteItemInput, op ...request.Option) (*dynamodb.BatchWriteItemOutput, error)
	TransactWriteItemsWithContextFn func(aws.Context, *dynamodb.TransactWriteItemsInput, ...request.Option) (*dynamodb.TransactWriteItemsOutput, error)
	dynamodbiface.DynamoDBAPI
}

type DynamoDBItem struct {
	Key               string `json:"key"`
	Value             string `json:"value"`
	TestAttributeName int64  `json:"testAttributeName"`
}

const (
	tableName = "table_name"
	pkey      = "partitionKey"
)

func (m *mockedDynamoDB) GetItemWithContext(ctx context.Context, input *dynamodb.GetItemInput, op ...request.Option) (*dynamodb.GetItemOutput, error) {
	return m.GetItemWithContextFn(ctx, input, op...)
}

func (m *mockedDynamoDB) PutItemWithContext(ctx context.Context, input *dynamodb.PutItemInput, op ...request.Option) (*dynamodb.PutItemOutput, error) {
	return m.PutItemWithContextFn(ctx, input, op...)
}

func (m *mockedDynamoDB) DeleteItemWithContext(ctx context.Context, input *dynamodb.DeleteItemInput, op ...request.Option) (*dynamodb.DeleteItemOutput, error) {
	return m.DeleteItemWithContextFn(ctx, input, op...)
}

func (m *mockedDynamoDB) BatchWriteItemWithContext(ctx context.Context, input *dynamodb.BatchWriteItemInput, op ...request.Option) (*dynamodb.BatchWriteItemOutput, error) {
	return m.BatchWriteItemWithContextFn(ctx, input, op...)
}

func (m *mockedDynamoDB) TransactWriteItemsWithContext(ctx context.Context, input *dynamodb.TransactWriteItemsInput, op ...request.Option) (*dynamodb.TransactWriteItemsOutput, error) {
	return m.TransactWriteItemsWithContextFn(ctx, input, op...)
}

func TestInit(t *testing.T) {
	m := state.Metadata{}
	s := &StateStore{
		partitionKey: defaultPartitionKeyName,
	}

	t.Run("NewDynamoDBStateStore Default Partition Key", func(t *testing.T) {
		assert.NotNil(t, s)
		assert.Equal(t, s.partitionKey, defaultPartitionKeyName)
	})

	t.Run("Init with valid metadata", func(t *testing.T) {
		m.Properties = map[string]string{
			"AccessKey":        "a",
			"Region":           "eu-west-1",
			"SecretKey":        "a",
			"SessionToken":     "a",
			"Table":            "a",
			"TtlAttributeName": "a",
		}
		err := s.Init(context.Background(), m)
		assert.NoError(t, err)
	})

	t.Run("Init with missing table", func(t *testing.T) {
		m.Properties = map[string]string{
			"Dummy": "a",
		}
		err := s.Init(context.Background(), m)
		assert.Error(t, err)
		assert.Equal(t, err, fmt.Errorf("missing dynamodb table name"))
	})

	t.Run("Init with valid table", func(t *testing.T) {
		m.Properties = map[string]string{
			"Table":  "a",
			"Region": "eu-west-1",
		}
		err := s.Init(context.Background(), m)
		assert.NoError(t, err)
	})

	t.Run("Init with partition key", func(t *testing.T) {
		pkey := "pkey"
		m.Properties = map[string]string{
			"Table":        "a",
			"partitionKey": pkey,
		}
		err := s.Init(context.Background(), m)
		assert.NoError(t, err)
		assert.Equal(t, s.partitionKey, pkey)
	})
}

func TestGet(t *testing.T) {
	t.Run("Successfully retrieve item", func(t *testing.T) {
		ss := &StateStore{
			partitionKey: defaultPartitionKeyName,
		}
		ss.client = &mockedDynamoDB{
			GetItemWithContextFn: func(ctx context.Context, input *dynamodb.GetItemInput, op ...request.Option) (output *dynamodb.GetItemOutput, err error) {
				return &dynamodb.GetItemOutput{
					Item: map[string]*dynamodb.AttributeValue{
						"key": {
							S: aws.String("someKey"),
						},
						"value": {
							S: aws.String("some value"),
						},
						"etag": {
							S: aws.String("1bdead4badc0ffee"),
						},
					},
				}, nil
			},
		}

		req := &state.GetRequest{
			Key:      "someKey",
			Metadata: nil,
			Options: state.GetStateOption{
				Consistency: "strong",
			},
		}
		out, err := ss.Get(context.Background(), req)
		assert.NoError(t, err)
		assert.Equal(t, []byte("some value"), out.Data)
		assert.Equal(t, "1bdead4badc0ffee", *out.ETag)
	})
	t.Run("Successfully retrieve item (with unexpired ttl)", func(t *testing.T) {
		ss := StateStore{
			client: &mockedDynamoDB{
				GetItemWithContextFn: func(ctx context.Context, input *dynamodb.GetItemInput, op ...request.Option) (output *dynamodb.GetItemOutput, err error) {
					return &dynamodb.GetItemOutput{
						Item: map[string]*dynamodb.AttributeValue{
							"key": {
								S: aws.String("someKey"),
							},
							"value": {
								S: aws.String("some value"),
							},
							"testAttributeName": {
								N: aws.String("4074862051"),
							},
							"etag": {
								S: aws.String("1bdead4badc0ffee"),
							},
						},
					}, nil
				},
			},
			ttlAttributeName: "testAttributeName",
		}
		req := &state.GetRequest{
			Key:      "someKey",
			Metadata: nil,
			Options: state.GetStateOption{
				Consistency: "strong",
			},
		}
		out, err := ss.Get(context.Background(), req)
		assert.NoError(t, err)
		assert.Equal(t, []byte("some value"), out.Data)
		assert.Equal(t, "1bdead4badc0ffee", *out.ETag)
	})
	t.Run("Successfully retrieve item (with expired ttl)", func(t *testing.T) {
		ss := StateStore{
			client: &mockedDynamoDB{
				GetItemWithContextFn: func(ctx context.Context, input *dynamodb.GetItemInput, op ...request.Option) (output *dynamodb.GetItemOutput, err error) {
					return &dynamodb.GetItemOutput{
						Item: map[string]*dynamodb.AttributeValue{
							"key": {
								S: aws.String("someKey"),
							},
							"value": {
								S: aws.String("some value"),
							},
							"testAttributeName": {
								N: aws.String("35489251"),
							},
							"etag": {
								S: aws.String("1bdead4badc0ffee"),
							},
						},
					}, nil
				},
			},
			ttlAttributeName: "testAttributeName",
		}
		req := &state.GetRequest{
			Key:      "someKey",
			Metadata: nil,
			Options: state.GetStateOption{
				Consistency: "strong",
			},
		}
		out, err := ss.Get(context.Background(), req)
		assert.NoError(t, err)
		assert.Nil(t, out.Data)
		assert.Nil(t, out.ETag)
	})
	t.Run("Unsuccessfully get item", func(t *testing.T) {
		ss := StateStore{
			client: &mockedDynamoDB{
				GetItemWithContextFn: func(ctx context.Context, input *dynamodb.GetItemInput, op ...request.Option) (output *dynamodb.GetItemOutput, err error) {
					return nil, fmt.Errorf("failed to retrieve data")
				},
			},
		}
		req := &state.GetRequest{
			Key:      "key",
			Metadata: nil,
			Options: state.GetStateOption{
				Consistency: "strong",
			},
		}
		out, err := ss.Get(context.Background(), req)
		assert.Error(t, err)
		assert.Nil(t, out)
	})
	t.Run("Unsuccessfully with empty response", func(t *testing.T) {
		ss := StateStore{
			client: &mockedDynamoDB{
				GetItemWithContextFn: func(ctx context.Context, input *dynamodb.GetItemInput, op ...request.Option) (output *dynamodb.GetItemOutput, err error) {
					return &dynamodb.GetItemOutput{
						Item: map[string]*dynamodb.AttributeValue{},
					}, nil
				},
			},
		}
		req := &state.GetRequest{
			Key:      "key",
			Metadata: nil,
			Options: state.GetStateOption{
				Consistency: "strong",
			},
		}
		out, err := ss.Get(context.Background(), req)
		assert.NoError(t, err)
		assert.Nil(t, out.Data)
		assert.Nil(t, out.ETag)
	})
	t.Run("Unsuccessfully with no required key", func(t *testing.T) {
		ss := StateStore{
			client: &mockedDynamoDB{
				GetItemWithContextFn: func(ctx context.Context, input *dynamodb.GetItemInput, op ...request.Option) (output *dynamodb.GetItemOutput, err error) {
					return &dynamodb.GetItemOutput{
						Item: map[string]*dynamodb.AttributeValue{
							"value2": {
								S: aws.String("value"),
							},
						},
					}, nil
				},
			},
		}
		req := &state.GetRequest{
			Key:      "key",
			Metadata: nil,
			Options: state.GetStateOption{
				Consistency: "strong",
			},
		}
		out, err := ss.Get(context.Background(), req)
		assert.NoError(t, err)
		assert.Empty(t, out.Data)
	})
}

func TestSet(t *testing.T) {
	type value struct {
		Value string
	}

	t.Run("Successfully set item", func(t *testing.T) {
		ss := &StateStore{
			partitionKey: defaultPartitionKeyName,
		}
		ss.client = &mockedDynamoDB{
			PutItemWithContextFn: func(ctx context.Context, input *dynamodb.PutItemInput, op ...request.Option) (output *dynamodb.PutItemOutput, err error) {
				assert.Equal(t, dynamodb.AttributeValue{
					S: aws.String("key"),
				}, *input.Item["key"])
				assert.Equal(t, dynamodb.AttributeValue{
					S: aws.String(`{"Value":"value"}`),
				}, *input.Item["value"])
				assert.Equal(t, len(input.Item), 3)

				return &dynamodb.PutItemOutput{
					Attributes: map[string]*dynamodb.AttributeValue{
						"key": {
							S: aws.String("value"),
						},
					},
				}, nil
			},
		}
		req := &state.SetRequest{
			Key: "key",
			Value: value{
				Value: "value",
			},
		}
		err := ss.Set(context.Background(), req)
		assert.NoError(t, err)
	})

	t.Run("Successfully set item with matching etag", func(t *testing.T) {
		ss := &StateStore{
			partitionKey: defaultPartitionKeyName,
		}
		ss.client = &mockedDynamoDB{
			PutItemWithContextFn: func(ctx context.Context, input *dynamodb.PutItemInput, op ...request.Option) (output *dynamodb.PutItemOutput, err error) {
				assert.Equal(t, dynamodb.AttributeValue{
					S: aws.String("key"),
				}, *input.Item["key"])
				assert.Equal(t, dynamodb.AttributeValue{
					S: aws.String(`{"Value":"value"}`),
				}, *input.Item["value"])
				assert.Equal(t, "etag = :etag", *input.ConditionExpression)
				assert.Equal(t, &dynamodb.AttributeValue{
					S: aws.String("1bdead4badc0ffee"),
				}, input.ExpressionAttributeValues[":etag"])
				assert.Equal(t, len(input.Item), 3)

				return &dynamodb.PutItemOutput{
					Attributes: map[string]*dynamodb.AttributeValue{
						"key": {
							S: aws.String("value"),
						},
					},
				}, nil
			},
		}
		etag := "1bdead4badc0ffee"
		req := &state.SetRequest{
			ETag: &etag,
			Key:  "key",
			Value: value{
				Value: "value",
			},
		}
		err := ss.Set(context.Background(), req)
		assert.NoError(t, err)
	})

	t.Run("Unsuccessfully set item with mismatched etag", func(t *testing.T) {
		ss := &StateStore{
			partitionKey: defaultPartitionKeyName,
		}
		ss.client = &mockedDynamoDB{
			PutItemWithContextFn: func(ctx context.Context, input *dynamodb.PutItemInput, op ...request.Option) (output *dynamodb.PutItemOutput, err error) {
				assert.Equal(t, dynamodb.AttributeValue{
					S: aws.String("key"),
				}, *input.Item["key"])
				assert.Equal(t, dynamodb.AttributeValue{
					S: aws.String(`{"Value":"value"}`),
				}, *input.Item["value"])
				assert.Equal(t, "etag = :etag", *input.ConditionExpression)
				assert.Equal(t, &dynamodb.AttributeValue{
					S: aws.String("bogusetag"),
				}, input.ExpressionAttributeValues[":etag"])
				assert.Equal(t, len(input.Item), 3)

				var checkErr dynamodb.ConditionalCheckFailedException
				return nil, &checkErr
			},
		}
		etag := "bogusetag"
		req := &state.SetRequest{
			ETag: &etag,
			Key:  "key",
			Value: value{
				Value: "value",
			},
		}

		err := ss.Set(context.Background(), req)
		assert.Error(t, err)
		switch tagErr := err.(type) {
		case *state.ETagError:
			assert.Equal(t, tagErr.Kind(), state.ETagMismatch)
		default:
			assert.True(t, false)
		}
	})

	t.Run("Successfully set item with first-write-concurrency", func(t *testing.T) {
		ss := &StateStore{
			partitionKey: defaultPartitionKeyName,
		}
		ss.client = &mockedDynamoDB{
			PutItemWithContextFn: func(ctx context.Context, input *dynamodb.PutItemInput, op ...request.Option) (output *dynamodb.PutItemOutput, err error) {
				assert.Equal(t, dynamodb.AttributeValue{
					S: aws.String("key"),
				}, *input.Item["key"])
				assert.Equal(t, dynamodb.AttributeValue{
					S: aws.String(`{"Value":"value"}`),
				}, *input.Item["value"])
				assert.Equal(t, "attribute_not_exists(etag)", *input.ConditionExpression)
				assert.Equal(t, len(input.Item), 3)

				return &dynamodb.PutItemOutput{
					Attributes: map[string]*dynamodb.AttributeValue{
						"key": {
							S: aws.String("value"),
						},
					},
				}, nil
			},
		}
		req := &state.SetRequest{
			Key: "key",
			Value: value{
				Value: "value",
			},
			Options: state.SetStateOption{
				Concurrency: state.FirstWrite,
			},
		}
		err := ss.Set(context.Background(), req)
		assert.NoError(t, err)
	})

	t.Run("Unsuccessfully set item with first-write-concurrency", func(t *testing.T) {
		ss := &StateStore{
			partitionKey: defaultPartitionKeyName,
		}
		ss.client = &mockedDynamoDB{
			PutItemWithContextFn: func(ctx context.Context, input *dynamodb.PutItemInput, op ...request.Option) (output *dynamodb.PutItemOutput, err error) {
				assert.Equal(t, dynamodb.AttributeValue{
					S: aws.String("key"),
				}, *input.Item["key"])
				assert.Equal(t, dynamodb.AttributeValue{
					S: aws.String(`{"Value":"value"}`),
				}, *input.Item["value"])
				assert.Equal(t, "attribute_not_exists(etag)", *input.ConditionExpression)
				assert.Equal(t, len(input.Item), 3)

				var checkErr dynamodb.ConditionalCheckFailedException
				return nil, &checkErr
			},
		}
		req := &state.SetRequest{
			Key: "key",
			Value: value{
				Value: "value",
			},
			Options: state.SetStateOption{
				Concurrency: state.FirstWrite,
			},
		}
		err := ss.Set(context.Background(), req)
		assert.Error(t, err)
		switch err.(type) {
		case *state.ETagError:
			assert.True(t, false)
		default:
		}
	})

	t.Run("Successfully set item with ttl = -1", func(t *testing.T) {
		ss := &StateStore{
			partitionKey: defaultPartitionKeyName,
		}
		ss.client = &mockedDynamoDB{
			PutItemWithContextFn: func(ctx context.Context, input *dynamodb.PutItemInput, op ...request.Option) (output *dynamodb.PutItemOutput, err error) {
				assert.Equal(t, len(input.Item), 4)
				result := DynamoDBItem{}
				dynamodbattribute.UnmarshalMap(input.Item, &result)
				assert.Equal(t, result.Key, "someKey")
				assert.Equal(t, result.Value, "{\"Value\":\"someValue\"}")
				assert.Greater(t, result.TestAttributeName, time.Now().Unix()-2)
				assert.Less(t, result.TestAttributeName, time.Now().Unix())

				return &dynamodb.PutItemOutput{
					Attributes: map[string]*dynamodb.AttributeValue{
						"key": {
							S: aws.String("value"),
						},
					},
				}, nil
			},
		}
		ss.ttlAttributeName = "testAttributeName"

		req := &state.SetRequest{
			Key: "someKey",
			Value: value{
				Value: "someValue",
			},
			Metadata: map[string]string{
				"ttlInSeconds": "-1",
			},
		}
		err := ss.Set(context.Background(), req)
		assert.NoError(t, err)
	})
	t.Run("Successfully set item with 'correct' ttl", func(t *testing.T) {
		ss := &StateStore{
			partitionKey: defaultPartitionKeyName,
		}
		ss.client = &mockedDynamoDB{
			PutItemWithContextFn: func(ctx context.Context, input *dynamodb.PutItemInput, op ...request.Option) (output *dynamodb.PutItemOutput, err error) {
				assert.Equal(t, len(input.Item), 4)
				result := DynamoDBItem{}
				dynamodbattribute.UnmarshalMap(input.Item, &result)
				assert.Equal(t, result.Key, "someKey")
				assert.Equal(t, result.Value, "{\"Value\":\"someValue\"}")
				assert.Greater(t, result.TestAttributeName, time.Now().Unix()+180-1)
				assert.Less(t, result.TestAttributeName, time.Now().Unix()+180+1)

				return &dynamodb.PutItemOutput{
					Attributes: map[string]*dynamodb.AttributeValue{
						"key": {
							S: aws.String("value"),
						},
					},
				}, nil
			},
		}
		ss.ttlAttributeName = "testAttributeName"

		req := &state.SetRequest{
			Key: "someKey",
			Value: value{
				Value: "someValue",
			},
			Metadata: map[string]string{
				"ttlInSeconds": "180",
			},
		}
		err := ss.Set(context.Background(), req)
		assert.NoError(t, err)
	})

	t.Run("Unsuccessfully set item", func(t *testing.T) {
		ss := &StateStore{
			partitionKey: defaultPartitionKeyName,
		}
		ss.client = &mockedDynamoDB{
			PutItemWithContextFn: func(ctx context.Context, input *dynamodb.PutItemInput, op ...request.Option) (output *dynamodb.PutItemOutput, err error) {
				return nil, fmt.Errorf("unable to put item")
			},
		}
		req := &state.SetRequest{
			Key: "key",
			Value: value{
				Value: "value",
			},
		}
		err := ss.Set(context.Background(), req)
		assert.Error(t, err)
	})
	t.Run("Successfully set item with correct ttl but without component metadata", func(t *testing.T) {
		ss := &StateStore{
			partitionKey: defaultPartitionKeyName,
		}
		ss.client = &mockedDynamoDB{
			PutItemWithContextFn: func(ctx context.Context, input *dynamodb.PutItemInput, op ...request.Option) (output *dynamodb.PutItemOutput, err error) {
				assert.Equal(t, dynamodb.AttributeValue{
					S: aws.String("someKey"),
				}, *input.Item["key"])
				assert.Equal(t, dynamodb.AttributeValue{
					S: aws.String(`{"Value":"someValue"}`),
				}, *input.Item["value"])
				assert.Equal(t, len(input.Item), 3)

				return &dynamodb.PutItemOutput{
					Attributes: map[string]*dynamodb.AttributeValue{
						"key": {
							S: aws.String("value"),
						},
					},
				}, nil
			},
		}
		req := &state.SetRequest{
			Key: "someKey",
			Value: value{
				Value: "someValue",
			},
			Metadata: map[string]string{
				"ttlInSeconds": "180",
			},
		}
		err := ss.Set(context.Background(), req)
		assert.NoError(t, err)
	})
	t.Run("Unsuccessfully set item with ttl (invalid value)", func(t *testing.T) {
		ss := StateStore{
			client: &mockedDynamoDB{
				PutItemWithContextFn: func(ctx context.Context, input *dynamodb.PutItemInput, op ...request.Option) (output *dynamodb.PutItemOutput, err error) {
					assert.Equal(t, map[string]*dynamodb.AttributeValue{
						"key": {
							S: aws.String("somekey"),
						},
						"value": {
							S: aws.String(`{"Value":"somevalue"}`),
						},
						"ttlInSeconds": {
							N: aws.String("180"),
						},
					}, input.Item)

					return &dynamodb.PutItemOutput{
						Attributes: map[string]*dynamodb.AttributeValue{
							"key": {
								S: aws.String("value"),
							},
						},
					}, nil
				},
			},
			ttlAttributeName: "testAttributeName",
		}
		req := &state.SetRequest{
			Key: "somekey",
			Value: value{
				Value: "somevalue",
			},
			Metadata: map[string]string{
				"ttlInSeconds": "invalidvalue",
			},
		}
		err := ss.Set(context.Background(), req)
		assert.Error(t, err)
		assert.Equal(t, "dynamodb error: failed to parse ttlInSeconds: strconv.ParseInt: parsing \"invalidvalue\": invalid syntax", err.Error())
	})
}

func TestBulkSet(t *testing.T) {
	type value struct {
		Value string
	}

	t.Run("Successfully set items", func(t *testing.T) {
		ss := &StateStore{
			partitionKey: defaultPartitionKeyName,
		}
		ss.client = &mockedDynamoDB{
			BatchWriteItemWithContextFn: func(ctx context.Context, input *dynamodb.BatchWriteItemInput, op ...request.Option) (output *dynamodb.BatchWriteItemOutput, err error) {
				expected := map[string][]*dynamodb.WriteRequest{}
				expected[tableName] = []*dynamodb.WriteRequest{
					{
						PutRequest: &dynamodb.PutRequest{
							Item: map[string]*dynamodb.AttributeValue{
								"key": {
									S: aws.String("key1"),
								},
								"value": {
									S: aws.String(`{"Value":"value1"}`),
								},
							},
						},
					},
					{
						PutRequest: &dynamodb.PutRequest{
							Item: map[string]*dynamodb.AttributeValue{
								"key": {
									S: aws.String("key2"),
								},
								"value": {
									S: aws.String(`{"Value":"value2"}`),
								},
							},
						},
					},
				}

				for tbl := range expected {
					for reqNum := range expected[tbl] {
						expectedItem := expected[tbl][reqNum].PutRequest.Item
						inputItem := input.RequestItems[tbl][reqNum].PutRequest.Item

						assert.Equal(t, expectedItem["key"], inputItem["key"])
						assert.Equal(t, expectedItem["value"], inputItem["value"])
					}
				}

				return &dynamodb.BatchWriteItemOutput{
					UnprocessedItems: map[string][]*dynamodb.WriteRequest{},
				}, nil
			},
		}
		ss.table = tableName

		req := []state.SetRequest{
			{
				Key: "key1",
				Value: value{
					Value: "value1",
				},
			},
			{
				Key: "key2",
				Value: value{
					Value: "value2",
				},
			},
		}
		err := ss.BulkSet(context.Background(), req)
		assert.NoError(t, err)
	})
	t.Run("Successfully set items with ttl = -1", func(t *testing.T) {
		ss := &StateStore{
			partitionKey: defaultPartitionKeyName,
		}
		ss.client = &mockedDynamoDB{
			BatchWriteItemWithContextFn: func(ctx context.Context, input *dynamodb.BatchWriteItemInput, op ...request.Option) (output *dynamodb.BatchWriteItemOutput, err error) {
				expected := map[string][]*dynamodb.WriteRequest{}
				expected[tableName] = []*dynamodb.WriteRequest{
					{
						PutRequest: &dynamodb.PutRequest{
							Item: map[string]*dynamodb.AttributeValue{
								"key": {
									S: aws.String("key1"),
								},
								"value": {
									S: aws.String(`{"Value":"value1"}`),
								},
								"testAttributeName": {
									N: aws.String(strconv.FormatInt(time.Now().Unix()-1, 10)),
								},
							},
						},
					},
					{
						PutRequest: &dynamodb.PutRequest{
							Item: map[string]*dynamodb.AttributeValue{
								"key": {
									S: aws.String("key2"),
								},
								"value": {
									S: aws.String(`{"Value":"value2"}`),
								},
							},
						},
					},
				}
				for tbl := range expected {
					for reqNum := range expected[tbl] {
						expectedItem := expected[tbl][reqNum].PutRequest.Item
						inputItem := input.RequestItems[tbl][reqNum].PutRequest.Item

						assert.Equal(t, expectedItem["key"], inputItem["key"])
						assert.Equal(t, expectedItem["value"], inputItem["value"])
					}
				}

				return &dynamodb.BatchWriteItemOutput{
					UnprocessedItems: map[string][]*dynamodb.WriteRequest{},
				}, nil
			},
		}
		ss.table = tableName
		ss.ttlAttributeName = "testAttributeName"

		req := []state.SetRequest{
			{
				Key: "key1",
				Value: value{
					Value: "value1",
				},
				Metadata: map[string]string{
					"ttlInSeconds": "-1",
				},
			},
			{
				Key: "key2",
				Value: value{
					Value: "value2",
				},
			},
		}
		err := ss.BulkSet(context.Background(), req)
		assert.NoError(t, err)
	})
	t.Run("Successfully set items with ttl", func(t *testing.T) {
		ss := &StateStore{
			partitionKey: defaultPartitionKeyName,
		}
		ss.client = &mockedDynamoDB{
			BatchWriteItemWithContextFn: func(ctx context.Context, input *dynamodb.BatchWriteItemInput, op ...request.Option) (output *dynamodb.BatchWriteItemOutput, err error) {
				expected := map[string][]*dynamodb.WriteRequest{}
				// This might fail occasionally due to timestamp precision.
				timestamp := time.Now().Unix() + 90
				expected[tableName] = []*dynamodb.WriteRequest{
					{
						PutRequest: &dynamodb.PutRequest{
							Item: map[string]*dynamodb.AttributeValue{
								"key": {
									S: aws.String("key1"),
								},
								"value": {
									S: aws.String(`{"Value":"value1"}`),
								},
								"testAttributeName": {
									N: aws.String(strconv.FormatInt(timestamp, 10)),
								},
							},
						},
					},
					{
						PutRequest: &dynamodb.PutRequest{
							Item: map[string]*dynamodb.AttributeValue{
								"key": {
									S: aws.String("key2"),
								},
								"value": {
									S: aws.String(`{"Value":"value2"}`),
								},
							},
						},
					},
				}
				for tbl := range expected {
					for reqNum := range expected[tbl] {
						expectedItem := expected[tbl][reqNum].PutRequest.Item
						inputItem := input.RequestItems[tbl][reqNum].PutRequest.Item

						assert.Equal(t, expectedItem["key"], inputItem["key"])
						assert.Equal(t, expectedItem["value"], inputItem["value"])
					}
				}

				return &dynamodb.BatchWriteItemOutput{
					UnprocessedItems: map[string][]*dynamodb.WriteRequest{},
				}, nil
			},
		}
		ss.table = tableName
		ss.ttlAttributeName = "testAttributeName"

		req := []state.SetRequest{
			{
				Key: "key1",
				Value: value{
					Value: "value1",
				},
				Metadata: map[string]string{
					"ttlInSeconds": "90",
				},
			},
			{
				Key: "key2",
				Value: value{
					Value: "value2",
				},
			},
		}
		err := ss.BulkSet(context.Background(), req)
		assert.NoError(t, err)
	})
	t.Run("Unsuccessfully set items", func(t *testing.T) {
		ss := StateStore{
			client: &mockedDynamoDB{
				BatchWriteItemWithContextFn: func(ctx context.Context, input *dynamodb.BatchWriteItemInput, op ...request.Option) (output *dynamodb.BatchWriteItemOutput, err error) {
					return nil, fmt.Errorf("unable to bulk write items")
				},
			},
		}
		req := []state.SetRequest{
			{
				Key: "key",
				Value: value{
					Value: "value",
				},
			},
			{
				Key: "key",
				Value: value{
					Value: "value",
				},
			},
		}
		err := ss.BulkSet(context.Background(), req)
		assert.Error(t, err)
	})
}

func TestDelete(t *testing.T) {
	t.Run("Successfully delete item", func(t *testing.T) {
		req := &state.DeleteRequest{
			Key: "key",
		}

		ss := &StateStore{
			partitionKey: defaultPartitionKeyName,
		}
		ss.client = &mockedDynamoDB{
			DeleteItemWithContextFn: func(ctx context.Context, input *dynamodb.DeleteItemInput, op ...request.Option) (output *dynamodb.DeleteItemOutput, err error) {
				assert.Equal(t, map[string]*dynamodb.AttributeValue{
					"key": {
						S: aws.String(req.Key),
					},
				}, input.Key)

				return nil, nil
			},
		}

		err := ss.Delete(context.Background(), req)
		assert.NoError(t, err)
	})

	t.Run("Successfully delete item with matching etag", func(t *testing.T) {
		etag := "1bdead4badc0ffee"
		req := &state.DeleteRequest{
			ETag: &etag,
			Key:  "key",
		}
		ss := &StateStore{
			partitionKey: defaultPartitionKeyName,
		}
		ss.client = &mockedDynamoDB{
			DeleteItemWithContextFn: func(ctx context.Context, input *dynamodb.DeleteItemInput, op ...request.Option) (output *dynamodb.DeleteItemOutput, err error) {
				assert.Equal(t, map[string]*dynamodb.AttributeValue{
					"key": {
						S: aws.String(req.Key),
					},
				}, input.Key)
				assert.Equal(t, "etag = :etag", *input.ConditionExpression)
				assert.Equal(t, &dynamodb.AttributeValue{
					S: aws.String("1bdead4badc0ffee"),
				}, input.ExpressionAttributeValues[":etag"])

				return nil, nil
			},
		}

		err := ss.Delete(context.Background(), req)
		assert.NoError(t, err)
	})

	t.Run("Unsuccessfully delete item with mismatched etag", func(t *testing.T) {
		etag := "bogusetag"
		req := &state.DeleteRequest{
			ETag: &etag,
			Key:  "key",
		}

		ss := &StateStore{
			partitionKey: defaultPartitionKeyName,
		}
		ss.client = &mockedDynamoDB{
			DeleteItemWithContextFn: func(ctx context.Context, input *dynamodb.DeleteItemInput, op ...request.Option) (output *dynamodb.DeleteItemOutput, err error) {
				assert.Equal(t, map[string]*dynamodb.AttributeValue{
					"key": {
						S: aws.String(req.Key),
					},
				}, input.Key)
				assert.Equal(t, "etag = :etag", *input.ConditionExpression)
				assert.Equal(t, &dynamodb.AttributeValue{
					S: aws.String("bogusetag"),
				}, input.ExpressionAttributeValues[":etag"])

				var checkErr dynamodb.ConditionalCheckFailedException
				return nil, &checkErr
			},
		}

		err := ss.Delete(context.Background(), req)
		assert.Error(t, err)
		switch tagErr := err.(type) {
		case *state.ETagError:
			assert.Equal(t, tagErr.Kind(), state.ETagMismatch)
		default:
			assert.True(t, false)
		}
	})

	t.Run("Unsuccessfully delete item", func(t *testing.T) {
		ss := StateStore{
			client: &mockedDynamoDB{
				DeleteItemWithContextFn: func(ctx context.Context, input *dynamodb.DeleteItemInput, op ...request.Option) (output *dynamodb.DeleteItemOutput, err error) {
					return nil, fmt.Errorf("unable to delete item")
				},
			},
		}
		req := &state.DeleteRequest{
			Key: "key",
		}
		err := ss.Delete(context.Background(), req)
		assert.Error(t, err)
	})
}

func TestBulkDelete(t *testing.T) {
	t.Run("Successfully delete items", func(t *testing.T) {
		ss := &StateStore{
			partitionKey: defaultPartitionKeyName,
		}
		ss.client = &mockedDynamoDB{
			BatchWriteItemWithContextFn: func(ctx context.Context, input *dynamodb.BatchWriteItemInput, op ...request.Option) (output *dynamodb.BatchWriteItemOutput, err error) {
				expected := map[string][]*dynamodb.WriteRequest{}
				expected[tableName] = []*dynamodb.WriteRequest{
					{
						DeleteRequest: &dynamodb.DeleteRequest{
							Key: map[string]*dynamodb.AttributeValue{
								"key": {
									S: aws.String("key1"),
								},
							},
						},
					},
					{
						DeleteRequest: &dynamodb.DeleteRequest{
							Key: map[string]*dynamodb.AttributeValue{
								"key": {
									S: aws.String("key2"),
								},
							},
						},
					},
				}
				assert.Equal(t, expected, input.RequestItems)

				return &dynamodb.BatchWriteItemOutput{
					UnprocessedItems: map[string][]*dynamodb.WriteRequest{},
				}, nil
			},
		}
		ss.table = tableName

		req := []state.DeleteRequest{
			{
				Key: "key1",
			},
			{
				Key: "key2",
			},
		}
		err := ss.BulkDelete(context.Background(), req)
		assert.NoError(t, err)
	})
	t.Run("Unsuccessfully delete items", func(t *testing.T) {
		ss := StateStore{
			client: &mockedDynamoDB{
				BatchWriteItemWithContextFn: func(ctx context.Context, input *dynamodb.BatchWriteItemInput, op ...request.Option) (output *dynamodb.BatchWriteItemOutput, err error) {
					return nil, fmt.Errorf("unable to bulk write items")
				},
			},
		}
		req := []state.DeleteRequest{
			{
				Key: "key",
			},
			{
				Key: "key",
			},
		}
		err := ss.BulkDelete(context.Background(), req)
		assert.Error(t, err)
	})
}

func TestMultiTx(t *testing.T) {
	t.Run("Successfully Multiple Transaction Operations", func(t *testing.T) {
		ss := &StateStore{
			partitionKey: defaultPartitionKeyName,
		}
		firstKey := "key1"
		secondKey := "key2"
		secondValue := "value2"
		thirdKey := "key3"
		thirdValue := "value3"

		ops := []state.TransactionalStateOperation{
			state.DeleteRequest{
				Key: firstKey,
			},
			state.SetRequest{
				Key:   secondKey,
				Value: secondValue,
			},
			state.DeleteRequest{
				Key: secondKey,
			},
			state.SetRequest{
				Key:   thirdKey,
				Value: thirdValue,
			},
		}

		ss.client = &mockedDynamoDB{
			TransactWriteItemsWithContextFn: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, op ...request.Option) (*dynamodb.TransactWriteItemsOutput, error) {
				// ops - duplicates
				exOps := len(ops) - 1
				assert.Equal(t, exOps, len(input.TransactItems), "unexpected number of operations")

				txs := map[string]int{
					"P": 0,
					"D": 0,
				}
				for _, input := range input.TransactItems {
					switch {
					case input.Put != nil:
						txs["P"] += 1
					case input.Delete != nil:
						txs["D"] += 1
					}
				}
				assert.Equal(t, 1, txs["P"], "unexpected number of Put Operations")
				assert.Equal(t, 2, txs["D"], "unexpected number of Delete Operations")

				return &dynamodb.TransactWriteItemsOutput{}, nil
			},
		}
		ss.table = tableName

		req := &state.TransactionalStateRequest{
			Operations: ops,
			Metadata:   map[string]string{},
		}
		err := ss.Multi(context.Background(), req)
		assert.NoError(t, err)
	})
}
