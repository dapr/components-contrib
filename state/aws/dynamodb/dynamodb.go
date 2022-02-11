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
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	jsoniterator "github.com/json-iterator/go"

	aws_auth "github.com/dapr/components-contrib/authentication/aws"
	"github.com/dapr/components-contrib/state"
)

// StateStore is a DynamoDB state store.
type StateStore struct {
	client dynamodbiface.DynamoDBAPI
	table  string
}

type dynamoDBMetadata struct {
	Region       string `json:"region"`
	Endpoint     string `json:"endpoint"`
	AccessKey    string `json:"accessKey"`
	SecretKey    string `json:"secretKey"`
	SessionToken string `json:"sessionToken"`
	Table        string `json:"table"`
}

// NewDynamoDBStateStore returns a new dynamoDB state store.
func NewDynamoDBStateStore() state.Store {
	return &StateStore{}
}

// Init does metadata and connection parsing.
func (d *StateStore) Init(metadata state.Metadata) error {
	meta, err := d.getDynamoDBMetadata(metadata)
	if err != nil {
		return err
	}

	client, err := d.getClient(meta)
	if err != nil {
		return err
	}

	d.client = client
	d.table = meta.Table

	return nil
}

func (d *StateStore) Ping() error {
	return nil
}

// Features returns the features available in this state store.
func (d *StateStore) Features() []state.Feature {
	return nil
}

// Get retrieves a dynamoDB item.
func (d *StateStore) Get(req *state.GetRequest) (*state.GetResponse, error) {
	input := &dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(req.Options.Consistency == state.Strong),
		TableName:      aws.String(d.table),
		Key: map[string]*dynamodb.AttributeValue{
			"key": {
				S: aws.String(req.Key),
			},
		},
	}

	result, err := d.client.GetItem(input)
	if err != nil {
		return nil, err
	}

	if len(result.Item) == 0 {
		return &state.GetResponse{}, nil
	}

	var output string
	if err = dynamodbattribute.Unmarshal(result.Item["value"], &output); err != nil {
		return nil, err
	}

	return &state.GetResponse{
		Data: []byte(output),
	}, nil
}

// BulkGet performs a bulk get operations.
func (d *StateStore) BulkGet(req []state.GetRequest) (bool, []state.BulkGetResponse, error) {
	// TODO: replace with dynamodb.BatchGetItem for performance
	return false, nil, nil
}

// Set saves a dynamoDB item.
func (d *StateStore) Set(req *state.SetRequest) error {
	value, err := d.marshalToString(req.Value)
	if err != nil {
		return fmt.Errorf("dynamodb error: failed to set key %s: %s", req.Key, err)
	}

	ttl, err := d.parseTTL(req)
	if err != nil {
		return fmt.Errorf("dynamodb error: failed to parse ttlInSeconds: %s", err)
	}

	var item map[string]*dynamodb.AttributeValue
	if ttl != nil {
		item = map[string]*dynamodb.AttributeValue{
			"key": {
				S: aws.String(req.Key),
			},
			"value": {
				S: aws.String(value),
			},
			"expireAt": {
				N: aws.String(strconv.FormatInt(int64(*ttl), 10)),
			},
		}
	} else {
		item = map[string]*dynamodb.AttributeValue{
			"key": {
				S: aws.String(req.Key),
			},
			"value": {
				S: aws.String(value),
			},
		}
	}

	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: &d.table,
	}

	_, e := d.client.PutItem(input)

	return e
}

// BulkSet performs a bulk set operation.
func (d *StateStore) BulkSet(req []state.SetRequest) error {
	writeRequests := []*dynamodb.WriteRequest{}

	for _, r := range req {
		value, err := d.marshalToString(r.Value)
		if err != nil {
			return fmt.Errorf("dynamodb error: failed to set key %s: %s", r.Key, err)
		}
		ttl, err := d.parseTTL(&r)
		if err != nil {
			return fmt.Errorf("dynamodb error: failed to parse ttlInSeconds: %s", err)
		}

		var item map[string]*dynamodb.AttributeValue
		if ttl != nil {
			item = map[string]*dynamodb.AttributeValue{
				"key": {
					S: aws.String(r.Key),
				},
				"value": {
					S: aws.String(value),
				},
				"expireAt": {
					N: aws.String(strconv.FormatInt(int64(*ttl), 10)),
				},
			}
		} else {
			item = map[string]*dynamodb.AttributeValue{
				"key": {
					S: aws.String(r.Key),
				},
				"value": {
					S: aws.String(value),
				},
			}
		}

		writeRequest := &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: item,
			},
		}

		writeRequests = append(writeRequests, writeRequest)
	}

	requestItems := map[string][]*dynamodb.WriteRequest{}
	requestItems[d.table] = writeRequests

	_, e := d.client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: requestItems,
	})

	return e
}

// Delete performs a delete operation.
func (d *StateStore) Delete(req *state.DeleteRequest) error {
	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"key": {
				S: aws.String(req.Key),
			},
		},
		TableName: aws.String(d.table),
	}
	_, err := d.client.DeleteItem(input)

	return err
}

// BulkDelete performs a bulk delete operation.
func (d *StateStore) BulkDelete(req []state.DeleteRequest) error {
	writeRequests := []*dynamodb.WriteRequest{}

	for _, r := range req {
		writeRequest := &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: map[string]*dynamodb.AttributeValue{
					"key": {
						S: aws.String(r.Key),
					},
				},
			},
		}
		writeRequests = append(writeRequests, writeRequest)
	}

	requestItems := map[string][]*dynamodb.WriteRequest{}
	requestItems[d.table] = writeRequests

	_, e := d.client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: requestItems,
	})

	return e
}

func (d *StateStore) getDynamoDBMetadata(metadata state.Metadata) (*dynamoDBMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var meta dynamoDBMetadata
	err = json.Unmarshal(b, &meta)
	if err != nil {
		return nil, err
	}
	if meta.Table == "" {
		return nil, fmt.Errorf("missing dynamodb table name")
	}

	return &meta, nil
}

func (d *StateStore) getClient(metadata *dynamoDBMetadata) (*dynamodb.DynamoDB, error) {
	sess, err := aws_auth.GetClient(metadata.AccessKey, metadata.SecretKey, metadata.SessionToken, metadata.Region, metadata.Endpoint)
	if err != nil {
		return nil, err
	}
	c := dynamodb.New(sess)

	return c, nil
}

func (d *StateStore) marshalToString(v interface{}) (string, error) {
	if buf, ok := v.([]byte); ok {
		return string(buf), nil
	}

	return jsoniterator.ConfigFastest.MarshalToString(v)
}

//Parse and process ttlInSeconds
func (d *StateStore) parseTTL(req *state.SetRequest) (*int64, error) {
	if val, ok := req.Metadata["ttlInSeconds"]; ok && val != "" {
		parsedVal, err := strconv.ParseInt(val, 10, 0)
		if err != nil {
			return nil, err
		}
		parsedInt := int64(parsedVal)
		//To follow the spec
		if parsedInt == -1 {
			return nil, nil
		}
		// DynamoDB expects an epoch timestamp in seconds
		expirationTime := time.Now().Unix() + parsedInt

		return &experationTime, nil
	}

	return nil, nil
}
