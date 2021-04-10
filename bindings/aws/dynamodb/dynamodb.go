// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package dynamodb

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	aws_auth "github.com/dapr/components-contrib/authentication/aws"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

// DynamoDB allows performing stateful operations on AWS DynamoDB
type DynamoDB struct {
	client *dynamodb.DynamoDB
	table  string
	logger logger.Logger
}

type dynamoDBMetadata struct {
	Region       string `json:"region"`
	Endpoint     string `json:"endpoint"`
	AccessKey    string `json:"accessKey"`
	SecretKey    string `json:"secretKey"`
	SessionToken string `json:"sessionToken"`
	Table        string `json:"table"`
}

// NewDynamoDB returns a new DynamoDB instance
func NewDynamoDB(logger logger.Logger) *DynamoDB {
	return &DynamoDB{logger: logger}
}

// Init performs connection parsing for DynamoDB
func (d *DynamoDB) Init(metadata bindings.Metadata) error {
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

func (d *DynamoDB) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (d *DynamoDB) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var obj interface{}
	err := json.Unmarshal(req.Data, &obj)
	if err != nil {
		return nil, err
	}

	item, err := dynamodbattribute.MarshalMap(obj)
	if err != nil {
		return nil, err
	}

	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(d.table),
	}

	_, err = d.client.PutItem(input)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (d *DynamoDB) getDynamoDBMetadata(spec bindings.Metadata) (*dynamoDBMetadata, error) {
	b, err := json.Marshal(spec.Properties)
	if err != nil {
		return nil, err
	}

	var meta dynamoDBMetadata
	err = json.Unmarshal(b, &meta)
	if err != nil {
		return nil, err
	}

	return &meta, nil
}

func (d *DynamoDB) getClient(metadata *dynamoDBMetadata) (*dynamodb.DynamoDB, error) {
	sess, err := aws_auth.GetClient(metadata.AccessKey, metadata.SecretKey, metadata.SessionToken, metadata.Region, metadata.Endpoint)
	if err != nil {
		return nil, err
	}
	c := dynamodb.New(sess)

	return c, nil
}
