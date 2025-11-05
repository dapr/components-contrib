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

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	awsCommon "github.com/dapr/components-contrib/common/aws"
	awsCommonAuth "github.com/dapr/components-contrib/common/aws/auth"
	"github.com/dapr/components-contrib/metadata"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	jsoniterator "github.com/json-iterator/go"

	"github.com/dapr/components-contrib/common/utils"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
	"github.com/dapr/kit/ptr"
)

// StateStore is a DynamoDB state store.
type StateStore struct {
	state.BulkStore

	logger           logger.Logger
	table            string
	ttlAttributeName string
	partitionKey     string

	dynamodbClient awsCommon.DynamoDBClient
}

type dynamoDBMetadata struct {
	// Ignored by metadata parser because included in built-in authentication profile
	AccessKey    string `json:"accessKey" mapstructure:"accessKey" mdignore:"true"`
	SecretKey    string `json:"secretKey" mapstructure:"secretKey" mdignore:"true"`
	SessionToken string `json:"sessionToken"  mapstructure:"sessionToken" mdignore:"true"`

	// TODO: rm the alias in Dapr 1.17
	Region           string `json:"region" mapstructure:"region" mapstructurealiases:"awsRegion" mdignore:"true"`
	Endpoint         string `json:"endpoint"`
	Table            string `json:"table"`
	TTLAttributeName string `json:"ttlAttributeName"`
	PartitionKey     string `json:"partitionKey"`
}

type putData struct {
	ConditionExpression       *string
	ExpressionAttributeValues map[string]types.AttributeValue
	Item                      map[string]types.AttributeValue
	TableName                 *string
}

const (
	defaultPartitionKeyName = "key"
	metadataPartitionKey    = "partitionKey"
)

// NewDynamoDBStateStore returns a new dynamoDB state store.
func NewDynamoDBStateStore(logger logger.Logger) state.Store {
	s := &StateStore{
		partitionKey: defaultPartitionKeyName,
		logger:       logger,
	}
	s.BulkStore = state.NewDefaultBulkStore(s)
	return s
}

// Init does metadata and connection parsing
func (d *StateStore) Init(ctx context.Context, metadata state.Metadata) error {
	return d.InitWithOptions(ctx, metadata)
}

// InitWithOptions does metadata and connection parsing and extra aws options
func (d *StateStore) InitWithOptions(ctx context.Context, metadata state.Metadata, opts ...awsCommon.ConfigOption) error {
	meta, err := d.getDynamoDBMetadata(metadata)
	if err != nil {
		return err
	}

	configOpts := awsCommonAuth.Options{
		Logger: d.logger,

		Properties: metadata.Properties,

		Region:       meta.Region,
		Endpoint:     meta.Endpoint,
		AccessKey:    meta.AccessKey, // TODO: Remove fields which should be gleaned from metadata.properties
		SecretKey:    meta.SecretKey,
		SessionToken: meta.SessionToken,
	}

	if d.dynamodbClient == nil {
		awsConfig, err := awsCommon.NewConfig(ctx, configOpts, opts...)
		if err != nil {
			return err
		}

		d.dynamodbClient = dynamodb.NewFromConfig(awsConfig)
	}
	d.table = meta.Table
	d.ttlAttributeName = meta.TTLAttributeName
	d.partitionKey = meta.PartitionKey

	if err := d.validateTableAccess(ctx); err != nil {
		return fmt.Errorf("error validating DynamoDB table '%s' access: %w", d.table, err)
	}

	return nil
}

// validateConnection runs a dummy Get operation to validate the connection credentials,
// as well as validating that the table exists, and we have access to it
func (d *StateStore) validateTableAccess(ctx context.Context) error {
	input := &dynamodb.GetItemInput{
		ConsistentRead: ptr.Of(false),
		TableName:      ptr.Of(d.table),
		Key: map[string]types.AttributeValue{
			d.partitionKey: &types.AttributeValueMemberS{
				Value: utils.GetRandOrDefaultString("dapr-test-table"),
			},
		},
	}
	_, err := d.dynamodbClient.GetItem(ctx, input)
	return err
}

// Features returns the features available in this state store.
func (d *StateStore) Features() []state.Feature {
	// TTLs are enabled only if ttlAttributeName is set
	if d.ttlAttributeName == "" {
		return []state.Feature{
			state.FeatureETag,
			state.FeatureTransactional,
		}
	}

	return []state.Feature{
		state.FeatureETag,
		state.FeatureTransactional,
		state.FeatureTTL,
	}
}

// Get retrieves a dynamoDB item.
func (d *StateStore) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	input := &dynamodb.GetItemInput{
		ConsistentRead: ptr.Of(req.Options.Consistency == state.Strong),
		TableName:      ptr.Of(d.table),
		Key: map[string]types.AttributeValue{
			d.partitionKey: &types.AttributeValueMemberS{
				Value: req.Key,
			},
		},
	}
	result, err := d.dynamodbClient.GetItem(ctx, input)
	if err != nil {
		return nil, err
	}

	if len(result.Item) == 0 {
		return &state.GetResponse{}, nil
	}

	data, err := unmarshalValue(result.Item["value"])
	if err != nil {
		return nil, fmt.Errorf("dynamodb error: failed to unmarshal value for key %s: %w", req.Key, err)
	}

	var metadata map[string]string
	if d.ttlAttributeName != "" {
		if val, ok := result.Item[d.ttlAttributeName]; ok {
			var ttl int64
			if err = attributevalue.Unmarshal(val, &ttl); err != nil {
				return nil, err
			}
			if ttl <= time.Now().Unix() {
				// Item has expired but DynamoDB didn't delete it yet.
				return &state.GetResponse{}, nil
			}
			metadata = map[string]string{
				state.GetRespMetaKeyTTLExpireTime: time.Unix(ttl, 0).UTC().Format(time.RFC3339),
			}
		}
	}

	resp := &state.GetResponse{
		Data:     data,
		Metadata: metadata,
	}

	if result.Item["etag"] != nil {
		var etag string
		err = attributevalue.Unmarshal(result.Item["etag"], &etag)
		if err != nil {
			return nil, err
		}
		resp.ETag = &etag
	}

	return resp, nil
}

// Set saves a dynamoDB item.
func (d *StateStore) Set(ctx context.Context, req *state.SetRequest) error {
	pd, err := d.createPutData(req)
	if err != nil {
		return err
	}

	_, err = d.dynamodbClient.PutItem(ctx, pd.ToPutItemInput())
	if err != nil && req.HasETag() {
		var cErr *types.ConditionalCheckFailedException
		switch {
		case errors.As(err, &cErr):
			err = state.NewETagError(state.ETagMismatch, cErr)
		}
	}

	return err
}

// Delete performs a delete operation.
func (d *StateStore) Delete(ctx context.Context, req *state.DeleteRequest) error {
	input := &dynamodb.DeleteItemInput{
		Key: map[string]types.AttributeValue{
			d.partitionKey: &types.AttributeValueMemberS{
				Value: req.Key,
			},
		},
		TableName: ptr.Of(d.table),
	}

	if req.HasETag() {
		condExpr := "etag = :etag"
		input.ConditionExpression = &condExpr
		exprAttrValues := make(map[string]types.AttributeValue)
		exprAttrValues[":etag"] = &types.AttributeValueMemberS{
			Value: *req.ETag,
		}
		input.ExpressionAttributeValues = exprAttrValues
	}
	_, err := d.dynamodbClient.DeleteItem(ctx, input)
	if err != nil {
		var cErr *types.ConditionalCheckFailedException
		switch {
		case errors.As(err, &cErr):
			err = state.NewETagError(state.ETagMismatch, cErr)
		}
	}

	return err
}

func (d *StateStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := dynamoDBMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return
}

func (d *StateStore) Close() error {
	return nil
}

func (d *StateStore) getDynamoDBMetadata(meta state.Metadata) (*dynamoDBMetadata, error) {
	var m dynamoDBMetadata
	err := kitmd.DecodeMetadata(meta.Properties, &m)
	if m.Table == "" {
		return nil, errors.New("missing dynamodb table name")
	}
	m.PartitionKey = populatePartitionMetadata(meta.Properties, defaultPartitionKeyName)
	return &m, err
}

// createPutData creates a DynamoDB put request data from a SetRequest.
func (d *StateStore) createPutData(req *state.SetRequest) (putData, error) {
	item, err := d.createItem(req)
	if err != nil {
		return putData{}, err
	}

	pd := putData{
		Item:      item,
		TableName: ptr.Of(d.table),
	}

	if req.HasETag() {
		condExpr := "etag = :etag"
		pd.ConditionExpression = &condExpr
		exprAttrValues := make(map[string]types.AttributeValue)
		exprAttrValues[":etag"] = &types.AttributeValueMemberS{
			Value: *req.ETag,
		}
		pd.ExpressionAttributeValues = exprAttrValues
	} else if req.Options.Concurrency == state.FirstWrite {
		condExpr := "attribute_not_exists(etag)"
		pd.ConditionExpression = &condExpr
	}

	return pd, nil
}

func (d putData) ToPutItemInput() *dynamodb.PutItemInput {
	return &dynamodb.PutItemInput{
		ConditionExpression:       d.ConditionExpression,
		ExpressionAttributeValues: d.ExpressionAttributeValues,
		Item:                      d.Item,
		TableName:                 d.TableName,
	}
}

func (d putData) ToPut() *types.Put {
	return &types.Put{
		ConditionExpression:       d.ConditionExpression,
		ExpressionAttributeValues: d.ExpressionAttributeValues,
		Item:                      d.Item,
		TableName:                 d.TableName,
	}
}

// createItem creates a DynamoDB item from a SetRequest.
func (d *StateStore) createItem(req *state.SetRequest) (map[string]types.AttributeValue, error) {
	value, err := marshalValue(req.Value)
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

	item := map[string]types.AttributeValue{
		d.partitionKey: &types.AttributeValueMemberS{
			Value: req.Key,
		},
		"value": value,
		"etag": &types.AttributeValueMemberS{
			Value: strconv.FormatUint(newEtag, 16),
		},
	}

	if ttl != nil {
		item[d.ttlAttributeName] = &types.AttributeValueMemberN{
			Value: strconv.FormatInt(*ttl, 10),
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

func marshalValue(v interface{}) (types.AttributeValue, error) {
	if bt, ok := v.([]byte); ok {
		return &types.AttributeValueMemberB{Value: bt}, nil
	}

	str, err := jsoniterator.ConfigFastest.MarshalToString(v)
	if err != nil {
		return nil, err
	}

	return &types.AttributeValueMemberS{Value: str}, nil
}

func unmarshalValue(value types.AttributeValue) ([]byte, error) {
	if value == nil {
		return []byte(nil), nil
	}

	var bytes []byte
	if err := attributevalue.Unmarshal(value, &bytes); err == nil {
		return bytes, nil
	}

	var str string
	if err := attributevalue.Unmarshal(value, &str); err == nil {
		return []byte(str), nil
	}

	return nil, fmt.Errorf("unsupported attribute value type %T", value)
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

// MultiMaxSize returns the maximum number of operations allowed in a transaction.
// For AWS DynamoDB, that's 100.
func (d *StateStore) MultiMaxSize() int {
	return 100
}

// Multi performs a transactional operation. succeeds only if all operations succeed, and fails if one or more operations fail.
func (d *StateStore) Multi(ctx context.Context, request *state.TransactionalStateRequest) error {
	opns := len(request.Operations)
	if opns == 0 {
		return nil
	}

	twinput := &dynamodb.TransactWriteItemsInput{
		TransactItems: make([]types.TransactWriteItem, 0, opns),
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

		twi := types.TransactWriteItem{}
		switch req := o.(type) {
		case state.SetRequest:
			pd, err := d.createPutData(&req)
			if err != nil {
				return fmt.Errorf("dynamodb error: failed to marshal value for key %s: %w", req.Key, err)
			}
			twi.Put = pd.ToPut()

		case state.DeleteRequest:
			twi.Delete = &types.Delete{
				TableName: ptr.Of(d.table),
				Key: map[string]types.AttributeValue{
					d.partitionKey: &types.AttributeValueMemberS{
						Value: req.Key,
					},
				},
			}
		}
		twinput.TransactItems = append(twinput.TransactItems, twi)
	}
	_, err := d.dynamodbClient.TransactWriteItems(ctx, twinput)

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
