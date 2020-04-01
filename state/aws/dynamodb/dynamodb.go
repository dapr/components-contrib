package dynamodb

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/dapr/components-contrib/state"
)

// StateStore is a DynamoDB state store
type StateStore struct {
	client dynamodbiface.DynamoDBAPI
	table  string
}

type dynamoDBMetadata struct {
	Region       string `json:"region"`
	AccessKey    string `json:"accessKey"`
	SecretKey    string `json:"secretKey"`
	SessionToken string `json:"sessionToken"`
	Table        string `json:"table"`
}

// NewDynamoDBStateStore returns a new dynamoDB state store
func NewDynamoDBStateStore() *StateStore {
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

// Set saves a dynamoDB item
func (d *StateStore) Set(req *state.SetRequest) error {
	item := map[string]*dynamodb.AttributeValue{
		"key": {
			S: aws.String(req.Key),
		},
		"value": {
			S: aws.String(fmt.Sprintf("%v", req.Value)),
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
	for _, r := range req {
		err := d.Set(&r)
		if err != nil {
			return err
		}
	}
	return nil
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
	for _, r := range req {
		err := d.Delete(&r)
		if err != nil {
			return err
		}
	}
	return nil
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

	if meta.SecretKey == "" || meta.AccessKey == "" || meta.Region == "" || meta.SessionToken == "" {
		return nil, fmt.Errorf("missing aws credentials in metadata")
	}

	return &meta, nil
}

func (d *StateStore) getClient(meta *dynamoDBMetadata) (*dynamodb.DynamoDB, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(meta.Region),
		Credentials: credentials.NewStaticCredentials(meta.AccessKey, meta.SecretKey, meta.SessionToken),
	})
	if err != nil {
		return nil, err
	}

	c := dynamodb.New(sess)
	return c, nil
}
