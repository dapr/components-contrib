/*
Copyright 2022 The Dapr Authors
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

package dynamoDBStorage_test

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

const (
	tableName = "tableName"
)

func createTestTables(tables []map[string]string) error {
	svc := dynamoDBService()
	for _, t := range tables {
		if err := createTable(svc, t); err != nil {
			return err
		}
	}
	return nil
}

func deleteTestTables(tables []string) error {
	svc := dynamoDBService()
	for _, t := range tables {
		if err := deleteTable(svc, t); err != nil {
			return err
		}
	}
	return nil
}

func dynamoDBService() dynamodbiface.DynamoDBAPI {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	return dynamodb.New(sess)
}

func createTable(svc dynamodbiface.DynamoDBAPI, table map[string]string) error {

	attributeDefinitions := []*dynamodb.AttributeDefinition{
		{
			AttributeName: aws.String(table[key]),
			AttributeType: aws.String("S"),
		},
	}

	keySchema := []*dynamodb.KeySchemaElement{
		{
			AttributeName: aws.String(table[key]),
			KeyType:       aws.String("HASH"),
		},
	}

	provisionedThroughput := &dynamodb.ProvisionedThroughput{
		ReadCapacityUnits:  aws.Int64(10),
		WriteCapacityUnits: aws.Int64(10),
	}

	_, err := svc.CreateTable(&dynamodb.CreateTableInput{
		AttributeDefinitions:  attributeDefinitions,
		KeySchema:             keySchema,
		ProvisionedThroughput: provisionedThroughput,
		TableName:             aws.String(table[tableName]),
	})
	if err != nil {
		return err
	}

	return svc.WaitUntilTableExists(&dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
}

func deleteTable(svc dynamodbiface.DynamoDBAPI, tableName string) error {
	_, err := svc.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && (aerr.Code() != dynamodb.ErrCodeResourceNotFoundException) {
			return err
		}
	}

	return nil
}
