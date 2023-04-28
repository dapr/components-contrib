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
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	jsoniterator "github.com/json-iterator/go"

	awsAuth "github.com/dapr/components-contrib/internal/authentication/aws"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

// StateStore is a DynamoDB state store.
type StateStore struct {
	state.BulkStore

	client           dynamodbiface.DynamoDBAPI
	table            string
	ttlAttributeName string
	partitionKey     string
}

type dynamoDBMetadata struct {
	Region           string `json:"region"`
	Endpoint         string `json:"endpoint"`
	AccessKey        string `json:"accessKey"`
	SecretKey        string `json:"secretKey"`
	SessionToken     string `json:"sessionToken"`
	Table            string `json:"table"`
	TTLAttributeName string `json:"ttlAttributeName"`
	PartitionKey     string `json:"partitionKey"`
}

const (
	defaultPartitionKeyName = "key"
	metadataPartitionKey    = "partitionKey"
)

// NewDynamoDBStateStore returns a new dynamoDB state store.
func NewDynamoDBStateStore(_ logger.Logger) state.Store {
	s := &StateStore{
		partitionKey: defaultPartitionKeyName,
	}
	s.BulkStore = state.NewDefaultBulkStore(s)
	return s
}

// Init does metadata and connection parsing.
func (d *StateStore) Init(_ context.Context, metadata state.Metadata) error {
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
	d.ttlAttributeName = meta.TTLAttributeName
	d.partitionKey = meta.PartitionKey

	return nil
}

// Features returns the features available in this state store.
func (d *StateStore) Features() []state.Feature {
	return []state.Feature{state.FeatureETag, state.FeatureTransactional}
}

// Get retrieves a dynamoDB item.
func (d *StateStore) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	input := &dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(req.Options.Consistency == state.Strong),
		TableName:      aws.String(d.table),
		Key: map[string]*dynamodb.AttributeValue{
			d.partitionKey: {
				S: aws.String(req.Key),
			},
		},
	}

	result, err := d.client.GetItemWithContext(ctx, input)
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

	var ttl int64
	if d.ttlAttributeName != "" {
		if val, ok := result.Item[d.ttlAttributeName]; ok {
			if err = dynamodbattribute.Unmarshal(val, &ttl); err != nil {
				return nil, err
			}
			if ttl <= time.Now().Unix() {
				// Item has expired but DynamoDB didn't delete it yet.
				return &state.GetResponse{}, nil
			}
		}
	}

	resp := &state.GetResponse{
		Data: []byte(output),
	}

	var etag string
	if etagVal, ok := result.Item["etag"]; ok {
		if err = dynamodbattribute.Unmarshal(etagVal, &etag); err != nil {
			return nil, err
		}
		resp.ETag = &etag
	}

	return resp, nil
}

// Set saves a dynamoDB item.
func (d *StateStore) Set(ctx context.Context, req *state.SetRequest) error {
	item, err := d.getItemFromReq(req)
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: &d.table,
	}

	haveEtag := false
	if req.ETag != nil && *req.ETag != "" {
		haveEtag = true
		condExpr := "etag = :etag"
		input.ConditionExpression = &condExpr
		exprAttrValues := make(map[string]*dynamodb.AttributeValue)
		exprAttrValues[":etag"] = &dynamodb.AttributeValue{
			S: req.ETag,
		}
		input.ExpressionAttributeValues = exprAttrValues
	} else if req.Options.Concurrency == state.FirstWrite {
		condExpr := "attribute_not_exists(etag)"
		input.ConditionExpression = &condExpr
	}

	_, err = d.client.PutItemWithContext(ctx, input)
	if err != nil && haveEtag {
		switch cErr := err.(type) {
		case *dynamodb.ConditionalCheckFailedException:
			err = state.NewETagError(state.ETagMismatch, cErr)
		}
	}

	return err
}

// BulkSet performs a bulk set operation.
func (d *StateStore) BulkSet(ctx context.Context, req []state.SetRequest) error {
	if len(req) == 1 {
		return d.Set(ctx, &req[0])
	}

	writeRequests := make([]*dynamodb.WriteRequest, len(req))
	for i := range req {
		if req[i].ETag != nil && *req[i].ETag != "" {
			return errors.New("dynamodb error: BulkSet() does not support etags; please use Set() instead")
		}
		if req[i].Options.Concurrency == state.FirstWrite {
			return errors.New("dynamodb error: BulkSet() does not support FirstWrite concurrency; please use Set() instead")
		}

		item, err := d.getItemFromReq(&req[i])
		if err != nil {
			return err
		}

		writeRequests[i] = &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: item,
			},
		}
	}

	requestItems := map[string][]*dynamodb.WriteRequest{
		d.table: writeRequests,
	}

	_, e := d.client.BatchWriteItemWithContext(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: requestItems,
	})

	return e
}

// Delete performs a delete operation.
func (d *StateStore) Delete(ctx context.Context, req *state.DeleteRequest) error {
	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			d.partitionKey: {
				S: aws.String(req.Key),
			},
		},
		TableName: aws.String(d.table),
	}

	if req.ETag != nil && *req.ETag != "" {
		condExpr := "etag = :etag"
		input.ConditionExpression = &condExpr
		exprAttrValues := make(map[string]*dynamodb.AttributeValue)
		exprAttrValues[":etag"] = &dynamodb.AttributeValue{
			S: req.ETag,
		}
		input.ExpressionAttributeValues = exprAttrValues
	}

	_, err := d.client.DeleteItemWithContext(ctx, input)
	if err != nil {
		switch cErr := err.(type) {
		case *dynamodb.ConditionalCheckFailedException:
			err = state.NewETagError(state.ETagMismatch, cErr)
		}
	}

	return err
}

// BulkDelete performs a bulk delete operation.
func (d *StateStore) BulkDelete(ctx context.Context, req []state.DeleteRequest) error {
	if len(req) == 1 {
		return d.Delete(ctx, &req[0])
	}

	writeRequests := make([]*dynamodb.WriteRequest, len(req))
	for i, r := range req {
		if r.ETag != nil && *r.ETag != "" {
			return errors.New("dynamodb error: BulkDelete() does not support etags; please use Delete() instead")
		}

		writeRequests[i] = &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: map[string]*dynamodb.AttributeValue{
					d.partitionKey: {
						S: aws.String(r.Key),
					},
				},
			},
		}
	}

	requestItems := map[string][]*dynamodb.WriteRequest{
		d.table: writeRequests,
	}

	_, e := d.client.BatchWriteItemWithContext(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: requestItems,
	})

	return e
}

func (d *StateStore) GetComponentMetadata() map[string]string {
	metadataStruct := dynamoDBMetadata{}
	metadataInfo := map[string]string{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return metadataInfo
}

func (d *StateStore) getDynamoDBMetadata(meta state.Metadata) (*dynamoDBMetadata, error) {
	var m dynamoDBMetadata
	err := metadata.DecodeMetadata(meta.Properties, &m)
	if m.Table == "" {
		return nil, errors.New("missing dynamodb table name")
	}
	m.PartitionKey = populatePartitionMetadata(meta.Properties, defaultPartitionKeyName)
	return &m, err
}

func (d *StateStore) getClient(metadata *dynamoDBMetadata) (*dynamodb.DynamoDB, error) {
	sess, err := awsAuth.GetClient(metadata.AccessKey, metadata.SecretKey, metadata.SessionToken, metadata.Region, metadata.Endpoint)
	if err != nil {
		return nil, err
	}
	c := dynamodb.New(sess)

	return c, nil
}

// getItemFromReq converts a dapr state.SetRequest into an dynamodb item
func (d *StateStore) getItemFromReq(req *state.SetRequest) (map[string]*dynamodb.AttributeValue, error) {
	value, err := d.marshalToString(req.Value)
	if err != nil {
		return nil, fmt.Errorf("dynamodb error: failed to marshal value for key %s: %w", req.Key, err)
	}

	ttl, err := d.parseTTL(req)
	if err != nil {
		return nil, fmt.Errorf("dynamodb error: failed to parse ttlInSeconds: %w", err)
	}

	newEtag, err := getRand64()
	if err != nil {
		return nil, fmt.Errorf("dynamodb error: failed to generate etag: %w", err)
	}

	item := map[string]*dynamodb.AttributeValue{
		d.partitionKey: {
			S: aws.String(req.Key),
		},
		"value": {
			S: aws.String(value),
		},
		"etag": {
			S: aws.String(strconv.FormatUint(newEtag, 16)),
		},
	}

	if ttl != nil {
		item[d.ttlAttributeName] = &dynamodb.AttributeValue{
			N: aws.String(strconv.FormatInt(*ttl, 10)),
		}
	}

	return item, nil
}

func getRand64() (uint64, error) {
	randBuf := make([]byte, 8)
	_, err := rand.Read(randBuf)
	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint64(randBuf), nil
}

func (d *StateStore) marshalToString(v interface{}) (string, error) {
	if buf, ok := v.([]byte); ok {
		return string(buf), nil
	}

	return jsoniterator.ConfigFastest.MarshalToString(v)
}

// Parse and process ttlInSeconds.
func (d *StateStore) parseTTL(req *state.SetRequest) (*int64, error) {
	// Only attempt to parse the value when TTL has been specified in component metadata.
	if d.ttlAttributeName != "" {
		if val, ok := req.Metadata["ttlInSeconds"]; ok && val != "" {
			parsedVal, err := strconv.ParseInt(val, 10, 0)
			if err != nil {
				return nil, err
			}
			// DynamoDB expects an epoch timestamp in seconds.
			expirationTime := time.Now().Unix() + parsedVal

			return &expirationTime, nil
		}
	}

	return nil, nil
}

// Multi performs a transactional operation. succeeds only if all operations succeed, and fails if one or more operations fail.
func (d *StateStore) Multi(ctx context.Context, request *state.TransactionalStateRequest) error {
	opns := len(request.Operations)
	if opns == 0 {
		return nil
	}

	twinput := &dynamodb.TransactWriteItemsInput{
		TransactItems: make([]*dynamodb.TransactWriteItem, 0, opns),
	}

	// Note: The following is a DynamoDB logic to avoid errors like following,
	// which happen when the same key is used in multiple operations within a Transaction:
	//
	//    ValidationException: Transaction request cannot include multiple operations on one item
	//
	// Dedup ops where the last operation with a matching Key takes precedence
	txs := map[string]int{}
	for i, o := range request.Operations {
		txs[o.GetKey()] = i
	}

	for i, o := range request.Operations {
		// skip operations removed in simulated set
		if txs[o.GetKey()] != i {
			continue
		}

		twi := &dynamodb.TransactWriteItem{}
		switch req := o.(type) {
		case state.SetRequest:
			value, err := d.marshalToString(req.Value)
			if err != nil {
				return fmt.Errorf("dynamodb error: failed to marshal value for key %s: %w", req.Key, err)
			}
			twi.Put = &dynamodb.Put{
				TableName: aws.String(d.table),
				Item: map[string]*dynamodb.AttributeValue{
					d.partitionKey: {
						S: aws.String(req.Key),
					},
					"value": {
						S: aws.String(value),
					},
				},
			}

		case state.DeleteRequest:
			twi.Delete = &dynamodb.Delete{
				TableName: aws.String(d.table),
				Key: map[string]*dynamodb.AttributeValue{
					d.partitionKey: {
						S: aws.String(req.Key),
					},
				},
			}
		}
		twinput.TransactItems = append(twinput.TransactItems, twi)
	}

	_, err := d.client.TransactWriteItemsWithContext(ctx, twinput)

	return err
}

// This is a helper to return the partition key to use.  If if metadata["partitionkey"] is present,
// use that, otherwise use default primay key "key".
func populatePartitionMetadata(requestMetadata map[string]string, defaultPartitionKeyName string) string {
	if val, found := requestMetadata[metadataPartitionKey]; found {
		return val
	}

	return defaultPartitionKeyName
}
