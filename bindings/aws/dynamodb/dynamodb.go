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
	"encoding/json"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/dapr/components-contrib/bindings"
	awsCommon "github.com/dapr/components-contrib/common/aws"
	awsCommonAuth "github.com/dapr/components-contrib/common/aws/auth"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

// DynamoDB allows performing stateful operations on AWS DynamoDB.
type DynamoDB struct {
	dynamodbClient awsCommon.DynamoDBClient
	table          string
	logger         logger.Logger
}

// TODO: the metadata fields need updating to use the builtin aws auth provider fully and reflect in metadata.yaml
type dynamoDBMetadata struct {
	Region       string `json:"region" mapstructure:"region"`
	Endpoint     string `json:"endpoint" mapstructure:"endpoint"`
	AccessKey    string `json:"accessKey" mapstructure:"accessKey"`
	SecretKey    string `json:"secretKey" mapstructure:"secretKey"`
	SessionToken string `json:"sessionToken" mapstructure:"sessionToken"`
	Table        string `json:"table" mapstructure:"table"`
}

// NewDynamoDB returns a new DynamoDB instance.
func NewDynamoDB(logger logger.Logger) bindings.OutputBinding {
	return &DynamoDB{logger: logger}
}

// Init performs connection parsing for DynamoDB.
func (d *DynamoDB) Init(ctx context.Context, metadata bindings.Metadata) error {
	meta, err := d.getDynamoDBMetadata(metadata)
	if err != nil {
		return err
	}

	opts := awsCommonAuth.Options{
		Logger:       d.logger,
		Properties:   metadata.Properties,
		Region:       meta.Region,
		Endpoint:     meta.Endpoint,
		AccessKey:    meta.AccessKey,
		SecretKey:    meta.SecretKey,
		SessionToken: meta.SessionToken,
	}

	awsConfig, err := awsCommon.NewConfig(ctx, opts)
	if err != nil {
		return err
	}

	d.dynamodbClient = dynamodb.NewFromConfig(awsConfig)
	d.table = meta.Table

	return nil
}

func (d *DynamoDB) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (d *DynamoDB) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var obj interface{}
	err := json.Unmarshal(req.Data, &obj)
	if err != nil {
		return nil, err
	}

	item, err := attributevalue.MarshalMap(obj)
	if err != nil {
		return nil, err
	}

	_, err = d.dynamodbClient.PutItem(ctx, &dynamodb.PutItemInput{
		Item:      item,
		TableName: &d.table,
	})
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (d *DynamoDB) getDynamoDBMetadata(spec bindings.Metadata) (*dynamoDBMetadata, error) {
	var meta dynamoDBMetadata
	err := kitmd.DecodeMetadata(spec.Properties, &meta)
	if err != nil {
		return nil, err
	}

	return &meta, nil
}

// GetComponentMetadata returns the metadata of the component.
func (d *DynamoDB) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := dynamoDBMetadata{}
	if err := metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType); err != nil {
		if d != nil && d.logger != nil {
			d.logger.Errorf("error getting component metadata: %v", err)
		}
	}
	return
}

func (d *DynamoDB) Close() error {
	return nil
}
