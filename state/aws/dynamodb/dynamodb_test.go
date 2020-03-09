// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------
package dynamodb

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/dapr/components-contrib/state"

	"github.com/stretchr/testify/assert"
)

type mockedDynamoDB struct {
	GetItemFn    func(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	PutItemFn    func(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	DeleteItemFn func(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error)
	dynamodbiface.DynamoDBAPI
}

func (m *mockedDynamoDB) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	return m.GetItemFn(input)
}

func (m *mockedDynamoDB) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	return m.PutItemFn(input)
}

func (m *mockedDynamoDB) DeleteItem(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	return m.DeleteItemFn(input)
}

func TestInit(t *testing.T) {
	m := state.Metadata{}
	s := NewDynamoDBStateStore()
	t.Run("Init with valid metadata", func(t *testing.T) {
		m.Properties = map[string]string{
			"AccessKey":    "a",
			"Region":       "a",
			"SecretKey":    "a",
			"SessionToken": "a",
		}
		err := s.Init(m)
		assert.Nil(t, err)
	})

	t.Run("Init with missing metadata", func(t *testing.T) {
		m.Properties = map[string]string{
			"Dummy": "a",
		}
		err := s.Init(m)
		assert.NotNil(t, err)
		assert.Equal(t, err, fmt.Errorf("missing aws credentials in metadata"))
	})
}

func TestGet(t *testing.T) {
	t.Run("Successfully retrieve item", func(t *testing.T) {
		ss := StateStore{
			client: &mockedDynamoDB{
				GetItemFn: func(input *dynamodb.GetItemInput) (output *dynamodb.GetItemOutput, err error) {
					return &dynamodb.GetItemOutput{
						Item: map[string]*dynamodb.AttributeValue{
							"key": {
								S: aws.String("key"),
							},
							"value": {
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
		out, err := ss.Get(req)
		assert.Nil(t, err)
		assert.Equal(t, []byte("value"), out.Data)
	})
	t.Run("Unsuccessfully get item", func(t *testing.T) {
		ss := StateStore{
			client: &mockedDynamoDB{
				GetItemFn: func(input *dynamodb.GetItemInput) (output *dynamodb.GetItemOutput, err error) {
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
		out, err := ss.Get(req)
		assert.NotNil(t, err)
		assert.Nil(t, out)
	})
	t.Run("Unsuccessfully with empty response", func(t *testing.T) {
		ss := StateStore{
			client: &mockedDynamoDB{
				GetItemFn: func(input *dynamodb.GetItemInput) (output *dynamodb.GetItemOutput, err error) {
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
		out, err := ss.Get(req)
		assert.Nil(t, err)
		assert.Nil(t, out.Data)
	})
	t.Run("Unsuccessfully with no required key", func(t *testing.T) {
		ss := StateStore{
			client: &mockedDynamoDB{
				GetItemFn: func(input *dynamodb.GetItemInput) (output *dynamodb.GetItemOutput, err error) {
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
		out, err := ss.Get(req)
		assert.Nil(t, err)
		assert.Empty(t, out.Data)
	})
}

func TestSet(t *testing.T) {
	type value struct {
		Value string
	}

	t.Run("Successfully set item", func(t *testing.T) {
		ss := StateStore{
			client: &mockedDynamoDB{
				PutItemFn: func(input *dynamodb.PutItemInput) (output *dynamodb.PutItemOutput, err error) {
					assert.Equal(t, map[string]*dynamodb.AttributeValue{
						"key": {
							S: aws.String("key"),
						},
						"value": {
							S: aws.String("{value}"),
						}}, input.Item)
					return &dynamodb.PutItemOutput{
						Attributes: map[string]*dynamodb.AttributeValue{
							"key": {
								S: aws.String("value"),
							},
						},
					}, nil
				},
			},
		}
		req := &state.SetRequest{
			Key: "key",
			Value: value{
				Value: "value",
			},
		}
		err := ss.Set(req)
		assert.Nil(t, err)
	})
	t.Run("Un-successfully set item", func(t *testing.T) {
		ss := StateStore{
			client: &mockedDynamoDB{
				PutItemFn: func(input *dynamodb.PutItemInput) (output *dynamodb.PutItemOutput, err error) {
					return nil, fmt.Errorf("unable to put item")
				},
			},
		}
		req := &state.SetRequest{
			Key: "key",
			Value: value{
				Value: "value",
			},
		}
		err := ss.Set(req)
		assert.NotNil(t, err)
	})
}

func TestDelete(t *testing.T) {
	t.Run("Successfully delete item", func(t *testing.T) {
		req := &state.DeleteRequest{
			Key: "key",
		}

		ss := StateStore{
			client: &mockedDynamoDB{
				DeleteItemFn: func(input *dynamodb.DeleteItemInput) (output *dynamodb.DeleteItemOutput, err error) {
					assert.Equal(t, map[string]*dynamodb.AttributeValue{
						"key": {
							S: aws.String(req.Key),
						},
					}, input.Key)
					return nil, nil
				},
			},
		}
		err := ss.Delete(req)
		assert.Nil(t, err)
	})

	t.Run("Un-successfully delete item", func(t *testing.T) {
		ss := StateStore{
			client: &mockedDynamoDB{
				DeleteItemFn: func(input *dynamodb.DeleteItemInput) (output *dynamodb.DeleteItemOutput, err error) {
					return nil, fmt.Errorf("unable to delete item")
				},
			},
		}
		req := &state.DeleteRequest{
			Key: "key",
		}
		err := ss.Delete(req)
		assert.NotNil(t, err)
	})
}
