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
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	awsMock "github.com/dapr/components-contrib/common/aws/mock"
	"github.com/dapr/kit/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/state"
)

type DynamoDBItem struct {
	Key               string `json:"key"`
	Value             string `json:"value"`
	TestAttributeName int64  `json:"testAttributeName"`
}

const (
	tableName = "table_name"
	pkey      = "partitionKey"
)

var log logger.Logger

func init() {
	log = logger.NewLogger("dynamodb-state-store")
	log.SetOutputLevel(logger.DebugLevel)
}

func TestInit(t *testing.T) {
	m := state.Metadata{}

	s := StateStore{
		logger: log,
		dynamodbClient: &awsMock.DynamoDBClient{
			GetItemFn: func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
				return nil, nil
			},
		},
		partitionKey: defaultPartitionKeyName,
	}

	t.Run("NewDynamoDBStateStore Default Partition Key", func(t *testing.T) {
		assert.NotNil(t, s)
		assert.Equal(t, defaultPartitionKeyName, s.partitionKey)
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

		err := s.Init(t.Context(), m)
		require.NoError(t, err)
	})

	t.Run("Init with missing table", func(t *testing.T) {
		m.Properties = map[string]string{
			"Dummy": "a",
		}
		err := s.Init(t.Context(), m)
		require.Error(t, err)
		assert.Equal(t, err, errors.New("missing dynamodb table name"))
	})

	t.Run("Init with valid table", func(t *testing.T) {
		m.Properties = map[string]string{
			"Table":  "a",
			"Region": "eu-west-1",
		}
		err := s.Init(t.Context(), m)
		require.NoError(t, err)
	})

	t.Run("Init with partition key", func(t *testing.T) {
		pkey := "pkey"
		m.Properties = map[string]string{
			"Table":        "a",
			"partitionKey": pkey,
		}
		err := s.Init(t.Context(), m)
		require.NoError(t, err)
		assert.Equal(t, s.partitionKey, pkey)
	})

	t.Run("Init with bad table name or permissions", func(t *testing.T) {
		table := "does-not-exist"
		m.Properties = map[string]string{
			"Table": table,
		}

		s := StateStore{
			logger: log,
			dynamodbClient: &awsMock.DynamoDBClient{
				GetItemFn: func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
					return nil, errors.New("Requested resource not found")
				},
			},
			partitionKey: defaultPartitionKeyName,
			table:        table,
		}

		err := s.Init(t.Context(), m)
		require.Error(t, err)
		require.EqualError(t, err, "error validating DynamoDB table 'does-not-exist' access: Requested resource not found")
	})
}

func TestGet(t *testing.T) {
	t.Run("Successfully retrieve item", func(t *testing.T) {
		mockedDB := &awsMock.DynamoDBClient{
			GetItemFn: func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
				return &dynamodb.GetItemOutput{
					Item: map[string]types.AttributeValue{
						"key": &types.AttributeValueMemberS{
							Value: "someKey",
						},
						"value": &types.AttributeValueMemberS{
							Value: "some value",
						},
						"etag": &types.AttributeValueMemberS{
							Value: "1bdead4badc0ffee",
						},
					},
				}, nil
			},
		}

		s := StateStore{
			logger:         log,
			dynamodbClient: mockedDB,
			partitionKey:   defaultPartitionKeyName,
		}

		req := &state.GetRequest{
			Key:      "someKey",
			Metadata: nil,
			Options: state.GetStateOption{
				Consistency: "strong",
			},
		}
		out, err := s.Get(t.Context(), req)
		require.NoError(t, err)
		assert.Equal(t, []byte("some value"), out.Data)
		assert.Equal(t, "1bdead4badc0ffee", *out.ETag)
		assert.NotContains(t, out.Metadata, "ttlExpireTime")
	})
	t.Run("Successfully retrieve item (with unexpired ttl)", func(t *testing.T) {
		mockedDB := &awsMock.DynamoDBClient{
			GetItemFn: func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
				return &dynamodb.GetItemOutput{
					Item: map[string]types.AttributeValue{
						"key": &types.AttributeValueMemberS{
							Value: "someKey",
						},
						"value": &types.AttributeValueMemberS{
							Value: "some value",
						},
						"testAttributeName": &types.AttributeValueMemberN{
							Value: "4074862051",
						},
						"etag": &types.AttributeValueMemberS{
							Value: "1bdead4badc0ffee",
						},
					},
				}, nil
			},
		}

		s := StateStore{
			logger:           log,
			dynamodbClient:   mockedDB,
			ttlAttributeName: "testAttributeName",
		}
		req := &state.GetRequest{
			Key:      "someKey",
			Metadata: nil,
			Options: state.GetStateOption{
				Consistency: "strong",
			},
		}
		out, err := s.Get(t.Context(), req)
		require.NoError(t, err)
		assert.Equal(t, []byte("some value"), out.Data)
		assert.Equal(t, "1bdead4badc0ffee", *out.ETag)
		assert.Contains(t, out.Metadata, "ttlExpireTime")
		expireTime, err := time.Parse(time.RFC3339, out.Metadata["ttlExpireTime"])
		require.NoError(t, err)
		assert.Equal(t, int64(4074862051), expireTime.Unix())
	})

	t.Run("Successfully retrieve item (with expired ttl)", func(t *testing.T) {
		mockedDB := &awsMock.DynamoDBClient{
			GetItemFn: func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
				return &dynamodb.GetItemOutput{
					Item: map[string]types.AttributeValue{
						"key": &types.AttributeValueMemberS{
							Value: "someKey",
						},
						"value": &types.AttributeValueMemberS{
							Value: "some value",
						},
						"testAttributeName": &types.AttributeValueMemberN{
							Value: "35489251",
						},
						"etag": &types.AttributeValueMemberS{
							Value: "1bdead4badc0ffee",
						},
					},
				}, nil
			},
		}

		s := StateStore{
			logger:           log,
			dynamodbClient:   mockedDB,
			ttlAttributeName: "testAttributeName",
		}
		req := &state.GetRequest{
			Key:      "someKey",
			Metadata: nil,
			Options: state.GetStateOption{
				Consistency: "strong",
			},
		}
		out, err := s.Get(t.Context(), req)
		require.NoError(t, err)
		assert.Nil(t, out.Data)
		assert.Nil(t, out.ETag)
		assert.Nil(t, out.Metadata)
	})
	t.Run("Unsuccessfully get item", func(t *testing.T) {
		s := StateStore{
			dynamodbClient: &awsMock.DynamoDBClient{
				GetItemFn: func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
					return nil, errors.New("failed to retrieve data")
				},
			},
			logger: log,
		}

		req := &state.GetRequest{
			Key:      "key",
			Metadata: nil,
			Options: state.GetStateOption{
				Consistency: "strong",
			},
		}
		out, err := s.Get(t.Context(), req)
		require.Error(t, err)
		assert.Nil(t, out)
	})
	t.Run("Unsuccessfully with empty response", func(t *testing.T) {
		s := StateStore{
			dynamodbClient: &awsMock.DynamoDBClient{
				GetItemFn: func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
					return &dynamodb.GetItemOutput{
						Item: map[string]types.AttributeValue{},
					}, nil
				},
			},
			logger: log,
		}
		req := &state.GetRequest{
			Key:      "key",
			Metadata: nil,
			Options: state.GetStateOption{
				Consistency: "strong",
			},
		}
		out, err := s.Get(t.Context(), req)
		require.NoError(t, err)
		assert.Nil(t, out.Data)
		assert.Nil(t, out.ETag)
		assert.Nil(t, out.Metadata)
	})
	t.Run("Unsuccessfully with no required key", func(t *testing.T) {
		s := StateStore{
			dynamodbClient: &awsMock.DynamoDBClient{
				GetItemFn: func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
					return &dynamodb.GetItemOutput{
						Item: map[string]types.AttributeValue{
							"value2": &types.AttributeValueMemberS{
								Value: "value",
							},
						},
					}, nil
				},
			},
			logger: log,
		}
		req := &state.GetRequest{
			Key:      "key",
			Metadata: nil,
			Options: state.GetStateOption{
				Consistency: "strong",
			},
		}
		out, err := s.Get(t.Context(), req)
		require.NoError(t, err)
		assert.Empty(t, out.Data)
		assert.Nil(t, out.ETag)
	})
}

func TestSet(t *testing.T) {
	type value struct {
		Value string
	}

	t.Run("Successfully set item", func(t *testing.T) {
		mockedDB := &awsMock.DynamoDBClient{
			PutItemFn: func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
				assert.Equal(t, &types.AttributeValueMemberS{
					Value: "key",
				}, params.Item["key"])

				assert.Equal(t, &types.AttributeValueMemberS{
					Value: `{"Value":"value"}`,
				}, params.Item["value"])

				assert.Len(t, params.Item, 3)

				return &dynamodb.PutItemOutput{
					Attributes: map[string]types.AttributeValue{
						"key": &types.AttributeValueMemberS{
							Value: "value",
						},
					},
				}, nil
			},
		}

		s := StateStore{
			logger:         log,
			dynamodbClient: mockedDB,
			partitionKey:   defaultPartitionKeyName,
		}

		req := &state.SetRequest{
			Key: "key",
			Value: value{
				Value: "value",
			},
		}
		err := s.Set(t.Context(), req)
		require.NoError(t, err)
	})

	t.Run("Successfully set item with binary value", func(t *testing.T) {
		mockedDB := &awsMock.DynamoDBClient{
			PutItemFn: func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
				assert.Equal(t, &types.AttributeValueMemberS{
					Value: "key",
				}, params.Item["key"])

				assert.Equal(t, &types.AttributeValueMemberB{
					Value: []byte("value"),
				}, params.Item["value"])

				assert.Len(t, params.Item, 3)

				return &dynamodb.PutItemOutput{
					Attributes: map[string]types.AttributeValue{
						"key": &types.AttributeValueMemberS{
							Value: "value",
						},
					},
				}, nil
			},
		}

		s := StateStore{
			logger:         log,
			dynamodbClient: mockedDB,
			partitionKey:   defaultPartitionKeyName,
		}

		req := &state.SetRequest{
			Key:   "key",
			Value: []byte("value"),
		}
		err := s.Set(t.Context(), req)
		require.NoError(t, err)
	})

	t.Run("Successfully set item with binary value", func(t *testing.T) {
		mockedDB := &awsMock.DynamoDBClient{
			PutItemFn: func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
				assert.Equal(t, &types.AttributeValueMemberS{
					Value: "key",
				}, params.Item["key"])

				assert.Equal(t, &types.AttributeValueMemberB{
					Value: []byte("value"),
				}, params.Item["value"])

				assert.Len(t, params.Item, 3)

				return &dynamodb.PutItemOutput{
					Attributes: map[string]types.AttributeValue{
						"key": &types.AttributeValueMemberS{
							Value: "value",
						},
					},
				}, nil
			},
		}

		s := StateStore{
			logger:         log,
			dynamodbClient: mockedDB,
			partitionKey:   defaultPartitionKeyName,
		}

		req := &state.SetRequest{
			Key:   "key",
			Value: []byte("value"),
		}
		err := s.Set(t.Context(), req)

		require.NoError(t, err)
	})

	t.Run("Successfully set item with matching etag", func(t *testing.T) {
		mockedDB := &awsMock.DynamoDBClient{
			PutItemFn: func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
				assert.Equal(t, &types.AttributeValueMemberS{
					Value: "key",
				}, params.Item["key"])

				assert.Equal(t, &types.AttributeValueMemberS{
					Value: `{"Value":"value"}`,
				}, params.Item["value"])

				assert.Equal(t, "etag = :etag", *params.ConditionExpression)

				assert.Equal(t, &types.AttributeValueMemberS{
					Value: "1bdead4badc0ffee",
				}, params.ExpressionAttributeValues[":etag"])

				assert.Len(t, params.Item, 3)

				return &dynamodb.PutItemOutput{
					Attributes: map[string]types.AttributeValue{
						"key": &types.AttributeValueMemberS{
							Value: "value",
						},
					},
				}, nil
			},
		}

		s := StateStore{
			logger:         log,
			dynamodbClient: mockedDB,
			partitionKey:   defaultPartitionKeyName,
		}
		etag := "1bdead4badc0ffee"
		req := &state.SetRequest{
			ETag: &etag,
			Key:  "key",
			Value: value{
				Value: "value",
			},
		}
		err := s.Set(t.Context(), req)
		require.NoError(t, err)
	})

	t.Run("Unsuccessfully set item with mismatched etag", func(t *testing.T) {
		mockedDB := &awsMock.DynamoDBClient{
			PutItemFn: func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
				assert.Equal(t, &types.AttributeValueMemberS{
					Value: "key",
				}, params.Item["key"])

				assert.Equal(t, &types.AttributeValueMemberS{
					Value: `{"Value":"value"}`,
				}, params.Item["value"])

				assert.Equal(t, "etag = :etag", *params.ConditionExpression)

				assert.Equal(t, &types.AttributeValueMemberS{
					Value: "bogusetag",
				}, params.ExpressionAttributeValues[":etag"])

				assert.Len(t, params.Item, 3)

				var checkErr types.ConditionalCheckFailedException
				return nil, &checkErr
			},
		}

		s := StateStore{
			logger:         log,
			dynamodbClient: mockedDB,
			partitionKey:   defaultPartitionKeyName,
		}
		etag := "bogusetag"
		req := &state.SetRequest{
			ETag: &etag,
			Key:  "key",
			Value: value{
				Value: "value",
			},
		}

		err := s.Set(t.Context(), req)
		require.Error(t, err)
		switch tagErr := err.(type) {
		case *state.ETagError:
			assert.Equal(t, state.ETagMismatch, tagErr.Kind())
		default:
			assert.True(t, false)
		}
	})

	t.Run("Successfully set item with first-write-concurrency", func(t *testing.T) {
		mockedDB := &awsMock.DynamoDBClient{
			PutItemFn: func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
				assert.Equal(t, &types.AttributeValueMemberS{
					Value: "key",
				}, params.Item["key"])

				assert.Equal(t, &types.AttributeValueMemberS{
					Value: `{"Value":"value"}`,
				}, params.Item["value"])

				assert.Equal(t, "attribute_not_exists(etag)", *params.ConditionExpression)

				assert.Len(t, params.Item, 3)

				return &dynamodb.PutItemOutput{
					Attributes: map[string]types.AttributeValue{
						"key": &types.AttributeValueMemberS{
							Value: "value",
						},
					},
				}, nil
			},
		}

		s := StateStore{
			logger:         log,
			dynamodbClient: mockedDB,
			partitionKey:   defaultPartitionKeyName,
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
		err := s.Set(t.Context(), req)
		require.NoError(t, err)
	})

	t.Run("Unsuccessfully set item with first-write-concurrency", func(t *testing.T) {
		mockedDB := &awsMock.DynamoDBClient{
			PutItemFn: func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
				assert.Equal(t, &types.AttributeValueMemberS{
					Value: "key",
				}, params.Item["key"])

				assert.Equal(t, &types.AttributeValueMemberS{
					Value: `{"Value":"value"}`,
				}, params.Item["value"])

				assert.Equal(t, "attribute_not_exists(etag)", *params.ConditionExpression)

				assert.Len(t, params.Item, 3)

				var checkErr types.ConditionalCheckFailedException
				return nil, &checkErr
			},
		}

		s := StateStore{
			logger:         log,
			dynamodbClient: mockedDB,
			partitionKey:   defaultPartitionKeyName,
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
		err := s.Set(t.Context(), req)
		require.Error(t, err)
		switch err.(type) {
		case *state.ETagError:
			assert.True(t, false)
		default:
		}
	})

	t.Run("Successfully set item with ttl = -1", func(t *testing.T) {
		mockedDB := &awsMock.DynamoDBClient{
			PutItemFn: func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
				assert.Len(t, params.Item, 4)

				result := DynamoDBItem{}
				require.NoError(t, attributevalue.UnmarshalMap(params.Item, &result))

				assert.Equal(t, "someKey", result.Key)
				assert.JSONEq(t, "{\"Value\":\"someValue\"}", result.Value)
				assert.Greater(t, result.TestAttributeName, time.Now().Unix()-2)
				assert.Less(t, result.TestAttributeName, time.Now().Unix())

				return &dynamodb.PutItemOutput{
					Attributes: map[string]types.AttributeValue{
						"key": &types.AttributeValueMemberS{
							Value: "value",
						},
					},
				}, nil
			},
		}
		s := StateStore{
			logger:           log,
			dynamodbClient:   mockedDB,
			ttlAttributeName: "testAttributeName",
			partitionKey:     defaultPartitionKeyName,
		}

		req := &state.SetRequest{
			Key: "someKey",
			Value: value{
				Value: "someValue",
			},
			Metadata: map[string]string{
				"ttlInSeconds": "-1",
			},
		}
		err := s.Set(t.Context(), req)
		require.NoError(t, err)
	})

	t.Run("Successfully set item with 'correct' ttl", func(t *testing.T) {
		mockedDB := &awsMock.DynamoDBClient{
			PutItemFn: func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
				assert.Len(t, params.Item, 4)

				result := DynamoDBItem{}
				require.NoError(t, attributevalue.UnmarshalMap(params.Item, &result))

				assert.Equal(t, "someKey", result.Key)
				assert.JSONEq(t, "{\"Value\":\"someValue\"}", result.Value)
				assert.Greater(t, result.TestAttributeName, time.Now().Unix()+180-1)
				assert.Less(t, result.TestAttributeName, time.Now().Unix()+180+1)

				return &dynamodb.PutItemOutput{
					Attributes: map[string]types.AttributeValue{
						"key": &types.AttributeValueMemberS{
							Value: "value",
						},
					},
				}, nil
			},
		}

		s := StateStore{
			logger:           log,
			dynamodbClient:   mockedDB,
			partitionKey:     defaultPartitionKeyName,
			ttlAttributeName: "testAttributeName",
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
		err := s.Set(t.Context(), req)
		require.NoError(t, err)
	})

	t.Run("Unsuccessfully set item", func(t *testing.T) {
		mockedDB := &awsMock.DynamoDBClient{
			PutItemFn: func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
				return nil, errors.New("unable to put item")
			},
		}

		s := StateStore{
			logger:         log,
			dynamodbClient: mockedDB,
			partitionKey:   defaultPartitionKeyName,
		}
		req := &state.SetRequest{
			Key: "key",
			Value: value{
				Value: "value",
			},
		}
		err := s.Set(t.Context(), req)
		require.Error(t, err)
	})

	t.Run("Successfully set item with correct ttl but without component metadata", func(t *testing.T) {
		mockedDB := &awsMock.DynamoDBClient{
			PutItemFn: func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
				assert.Equal(t, &types.AttributeValueMemberS{
					Value: "someKey",
				}, params.Item["key"])

				assert.Equal(t, &types.AttributeValueMemberS{
					Value: `{"Value":"someValue"}`,
				}, params.Item["value"])

				assert.Len(t, params.Item, 3)

				return &dynamodb.PutItemOutput{
					Attributes: map[string]types.AttributeValue{
						"key": &types.AttributeValueMemberS{
							Value: "value",
						},
					},
				}, nil
			},
		}

		s := StateStore{
			logger:         log,
			dynamodbClient: mockedDB,
			partitionKey:   defaultPartitionKeyName,
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
		err := s.Set(t.Context(), req)
		require.NoError(t, err)
	})

	t.Run("Unsuccessfully set item with ttl (invalid value)", func(t *testing.T) {
		mockedDB := &awsMock.DynamoDBClient{
			PutItemFn: func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
				assert.Equal(t, map[string]types.AttributeValue{
					"key": &types.AttributeValueMemberS{
						Value: "somekey",
					},
					"value": &types.AttributeValueMemberS{
						Value: "somevalue",
					},
					"ttlInSeconds": &types.AttributeValueMemberN{
						Value: "100",
					},
				}, params.Item)

				return &dynamodb.PutItemOutput{
					Attributes: map[string]types.AttributeValue{
						"key": &types.AttributeValueMemberS{
							Value: "value",
						},
					},
				}, nil
			},
		}

		s := StateStore{
			logger:           log,
			dynamodbClient:   mockedDB,
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
		err := s.Set(t.Context(), req)
		require.Error(t, err)
		assert.Equal(t, "dynamodb error: failed to parse ttlInSeconds: strconv.ParseInt: parsing \"invalidvalue\": invalid syntax", err.Error())
	})
}

func TestDelete(t *testing.T) {
	t.Run("Successfully delete item", func(t *testing.T) {
		req := &state.DeleteRequest{
			Key: "key",
		}

		mockedDB := &awsMock.DynamoDBClient{
			DeleteItemFn: func(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
				assert.Equal(t, map[string]types.AttributeValue{
					"key": &types.AttributeValueMemberS{
						Value: req.Key,
					},
				}, params.Key)

				return nil, nil
			},
		}

		s := StateStore{
			logger:         log,
			dynamodbClient: mockedDB,
			partitionKey:   defaultPartitionKeyName,
		}

		err := s.Delete(t.Context(), req)
		require.NoError(t, err)
	})

	t.Run("Successfully delete item with matching etag", func(t *testing.T) {
		etag := "1bdead4badc0ffee"
		req := &state.DeleteRequest{
			ETag: &etag,
			Key:  "key",
		}

		mockedDB := &awsMock.DynamoDBClient{
			DeleteItemFn: func(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
				assert.Equal(t, map[string]types.AttributeValue{
					"key": &types.AttributeValueMemberS{
						Value: req.Key,
					},
				}, params.Key)
				assert.Equal(t, "etag = :etag", *params.ConditionExpression)
				assert.Equal(t, &types.AttributeValueMemberS{
					Value: "1bdead4badc0ffee",
				}, params.ExpressionAttributeValues[":etag"])

				return nil, nil
			},
		}
		s := StateStore{
			logger:         log,
			dynamodbClient: mockedDB,
			partitionKey:   defaultPartitionKeyName,
		}

		err := s.Delete(t.Context(), req)
		require.NoError(t, err)
	})

	t.Run("Unsuccessfully delete item with mismatched etag", func(t *testing.T) {
		etag := "bogusetag"
		req := &state.DeleteRequest{
			ETag: &etag,
			Key:  "key",
		}

		mockedDB := &awsMock.DynamoDBClient{
			DeleteItemFn: func(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
				assert.Equal(t, map[string]types.AttributeValue{
					"key": &types.AttributeValueMemberS{
						Value: req.Key,
					},
				}, params.Key)
				assert.Equal(t, "etag = :etag", *params.ConditionExpression)
				assert.Equal(t, &types.AttributeValueMemberS{
					Value: "bogusetag",
				}, params.ExpressionAttributeValues[":etag"])

				var checkErr types.ConditionalCheckFailedException
				return nil, &checkErr
			},
		}

		s := StateStore{
			logger:         log,
			dynamodbClient: mockedDB,
			partitionKey:   defaultPartitionKeyName,
		}
		err := s.Delete(t.Context(), req)
		require.Error(t, err)
		switch tagErr := err.(type) {
		case *state.ETagError:
			assert.Equal(t, state.ETagMismatch, tagErr.Kind())
		default:
			assert.True(t, false)
		}
	})

	t.Run("Unsuccessfully delete item", func(t *testing.T) {
		mockedDB := &awsMock.DynamoDBClient{
			DeleteItemFn: func(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
				return nil, errors.New("unable to delete item")
			},
		}

		s := StateStore{
			dynamodbClient: mockedDB,
			logger:         log,
		}

		req := &state.DeleteRequest{
			Key: "key",
		}
		err := s.Delete(t.Context(), req)
		require.Error(t, err)
	})
}

func TestMultiTx(t *testing.T) {
	t.Run("Successfully Multiple Transaction Operations", func(t *testing.T) {
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

		mockedDB := &awsMock.DynamoDBClient{
			TransactWriteItemsFn: func(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
				// ops - duplicates
				exOps := len(ops) - 1
				assert.Len(t, params.TransactItems, exOps, "unexpected number of operations")

				txs := map[string]int{
					"P": 0,
					"D": 0,
				}
				for _, input := range params.TransactItems {
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

		s := StateStore{
			logger:         log,
			dynamodbClient: mockedDB,
			table:          tableName,
			partitionKey:   defaultPartitionKeyName,
		}

		req := &state.TransactionalStateRequest{
			Operations: ops,
			Metadata:   map[string]string{},
		}
		err := s.Multi(t.Context(), req)
		require.NoError(t, err)
	})
}
