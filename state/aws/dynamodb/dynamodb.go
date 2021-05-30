package dynamodb

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	aws_auth "github.com/dapr/components-contrib/authentication/aws"
	"github.com/dapr/components-contrib/state"
	jsoniterator "github.com/json-iterator/go"
)

// StateStore is a DynamoDB state store
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

// NewDynamoDBStateStore returns a new dynamoDB state store
func NewDynamoDBStateStore() state.Store {
	return &StateStore{}
}

// Init does metadata and connection parsing
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

// Features returns the features available in this state store
func (d *StateStore) Features() []state.Feature {
	return nil
}

// Get retrieves a dynamoDB item
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

// BulkGet performs a bulk get operations
func (d *StateStore) BulkGet(req []state.GetRequest) (bool, []state.BulkGetResponse, error) {
	// TODO: replace with dynamodb.BatchGetItem for performance
	return false, nil, nil
}

// Set saves a dynamoDB item
func (d *StateStore) Set(req *state.SetRequest) error {
	value, err := d.marshalToString(req.Value)
	if err != nil {
		return fmt.Errorf("dynamodb error: failed to set key %s: %s", req.Key, err)
	}

	item := map[string]*dynamodb.AttributeValue{
		"key": {
			S: aws.String(req.Key),
		},
		"value": {
			S: aws.String(value),
		},
	}

	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: &d.table,
	}

	_, e := d.client.PutItem(input)

	return e
}

// BulkSet performs a bulk set operation
func (d *StateStore) BulkSet(req []state.SetRequest) error {
	writeRequests := []*dynamodb.WriteRequest{}

	for _, r := range req {
		value, err := d.marshalToString(r.Value)
		if err != nil {
			return fmt.Errorf("dynamodb error: failed to set key %s: %s", r.Key, err)
		}

		writeRequest := &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					"key": {
						S: aws.String(r.Key),
					},
					"value": {
						S: aws.String(value),
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

// Delete performs a delete operation
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

// BulkDelete performs a bulk delete operation
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
