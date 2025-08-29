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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	"github.com/dapr/components-contrib/bindings"
	awsAuth "github.com/dapr/components-contrib/common/authentication/aws"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

// DynamoDB allows performing stateful operations on AWS DynamoDB.
type DynamoDB struct {
	authProvider awsAuth.Provider
	table        string
	logger       logger.Logger
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

	opts := awsAuth.Options{
		Logger:       d.logger,
		Properties:   metadata.Properties,
		Region:       meta.Region,
		Endpoint:     meta.Endpoint,
		AccessKey:    meta.AccessKey,
		SecretKey:    meta.SecretKey,
		SessionToken: meta.SessionToken,
	}

	provider, err := awsAuth.NewProvider(ctx, opts, awsAuth.GetConfig(opts))
	if err != nil {
		return err
	}
	d.authProvider = provider
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

	item, err := dynamodbattribute.MarshalMap(obj)
	if err != nil {
		return nil, err
	}

	_, err = d.authProvider.DynamoDB().DynamoDB.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(d.table),
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
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}

func (d *DynamoDB) Close() error {
	if d.authProvider != nil {
		return d.authProvider.Close()
	}
	return nil
}
