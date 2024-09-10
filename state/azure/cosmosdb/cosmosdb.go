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

package cosmosdb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"

	"github.com/dapr/components-contrib/common/authentication/azure"
	"github.com/dapr/components-contrib/contenttype"
	contribmeta "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/query"
	stateutils "github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
	"github.com/dapr/kit/ptr"
)

// StateStore is a CosmosDB state store.
type StateStore struct {
	state.BulkStore

	client      *azcosmos.ContainerClient
	metadata    metadata
	contentType string
	logger      logger.Logger
}

type metadata struct {
	URL         string `json:"url"`
	MasterKey   string `json:"masterKey"`
	Database    string `json:"database"`
	Collection  string `json:"collection"`
	ContentType string `json:"contentType"`
}

type cosmosOperationType string

// CosmosOperation is a wrapper around a CosmosDB operation.
type CosmosOperation struct {
	Item CosmosItem          `json:"item"`
	Type cosmosOperationType `json:"type"`
}

// CosmosItem is a wrapper around a CosmosDB document.
type CosmosItem struct {
	ID           string      `json:"id"`
	Value        interface{} `json:"value"`
	IsBinary     bool        `json:"isBinary"`
	PartitionKey string      `json:"partitionKey"`
	TTL          *int        `json:"ttl,omitempty"`
	Etag         string      `json:"_etag"`
	TS           int64       `json:"_ts"`
}

const (
	metadataPartitionKey = "partitionKey"
	defaultTimeout       = 20 * time.Second
)

// Policy that makes all queries cross-partition
type crossPartitionQueryPolicy struct{}

func (p crossPartitionQueryPolicy) Do(req *policy.Request) (*http.Response, error) {
	raw := req.Raw()
	// Check if we're performing a query
	// In that case, remove the partitionkey header and enable cross-partition queries
	if strings.ToLower(raw.Header.Get("x-ms-documentdb-query")) == "true" {
		// Only when the partitionKey is fake (true), it will be removed amd enabled the cross partition
		if strings.ToLower(raw.Header.Get("x-ms-documentdb-partitionkey")) == "[true]" {
			raw.Header.Add("x-ms-documentdb-query-enablecrosspartition", "true")
			raw.Header.Del("x-ms-documentdb-partitionkey")
		}
	}
	return req.Next()
}

// NewCosmosDBStateStore returns a new CosmosDB state store.
func NewCosmosDBStateStore(logger logger.Logger) state.Store {
	s := &StateStore{
		logger: logger,
	}
	s.BulkStore = state.NewDefaultBulkStore(s)
	return s
}

func (c *StateStore) GetComponentMetadata() (metadataInfo contribmeta.MetadataMap) {
	metadataStruct := metadata{}
	contribmeta.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribmeta.StateStoreType)
	return
}

// Init does metadata and connection parsing.
func (c *StateStore) Init(ctx context.Context, meta state.Metadata) error {
	c.logger.Debugf("CosmosDB init start")

	m := metadata{
		ContentType: "application/json",
	}
	errDecode := kitmd.DecodeMetadata(meta.Properties, &m)
	if errDecode != nil {
		return errDecode
	}

	if m.URL == "" {
		return errors.New("url is required")
	}
	if m.Database == "" {
		return errors.New("database is required")
	}
	if m.Collection == "" {
		return errors.New("collection is required")
	}
	if m.ContentType == "" {
		return errors.New("contentType is required")
	}

	// Internal query policy was created due to lack of cross partition query capability in the current Go sdk
	opts := azcosmos.ClientOptions{
		ClientOptions: policy.ClientOptions{
			PerCallPolicies: []policy.Policy{
				crossPartitionQueryPolicy{},
			},
			Telemetry: policy.TelemetryOptions{
				ApplicationID: "dapr-" + logger.DaprVersion,
			},
		},
	}

	// Create the client; first, try authenticating with a master key, if present
	var client *azcosmos.Client
	if m.MasterKey != "" {
		var cred azcosmos.KeyCredential
		cred, err := azcosmos.NewKeyCredential(m.MasterKey)
		if err != nil {
			return err
		}
		client, err = azcosmos.NewClientWithKey(m.URL, cred, &opts)
		if err != nil {
			return err
		}
	} else {
		// Fallback to using Azure AD
		var env azure.EnvironmentSettings
		env, err := azure.NewEnvironmentSettings(meta.Properties)
		if err != nil {
			return err
		}
		token, tokenErr := env.GetTokenCredential()
		if tokenErr != nil {
			return tokenErr
		}
		client, err = azcosmos.NewClient(m.URL, token, &opts)
		if err != nil {
			return err
		}
	}
	// Create a container client
	dbClient, err := client.NewDatabase(m.Database)
	if err != nil {
		return err
	}
	// Container is synonymous with collection.
	dbContainer, err := dbClient.NewContainer(m.Collection)
	if err != nil {
		return err
	}
	c.client = dbContainer

	c.metadata = m
	c.contentType = m.ContentType

	readCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	_, err = c.client.Read(readCtx, nil)
	return err
}

// Features returns the features available in this state store.
func (c *StateStore) Features() []state.Feature {
	return []state.Feature{
		state.FeatureETag,
		state.FeatureTransactional,
		state.FeatureQueryAPI,
		state.FeatureTTL,
		state.FeaturePartitionKey,
	}
}

// Get retrieves a CosmosDB item.
func (c *StateStore) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	partitionKey := populatePartitionMetadata(req.Key, req.Metadata)

	options := azcosmos.ItemOptions{}
	if req.Options.Consistency == state.Strong {
		options.ConsistencyLevel = azcosmos.ConsistencyLevelSession.ToPtr()
	} else if req.Options.Consistency == state.Eventual {
		options.ConsistencyLevel = azcosmos.ConsistencyLevelEventual.ToPtr()
	}

	readCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	readItem, err := c.client.ReadItem(readCtx, azcosmos.NewPartitionKeyString(partitionKey), req.Key, &options)
	if err != nil {
		if isNotFoundError(err) {
			return &state.GetResponse{}, nil
		}
		return nil, err
	}

	item, err := NewCosmosItemFromResponse(readItem.Value, c.logger)
	if err != nil {
		return nil, err
	}

	var metadata map[string]string
	if item.TTL != nil {
		metadata = map[string]string{
			state.GetRespMetaKeyTTLExpireTime: time.Unix(item.TS+int64(*item.TTL), 0).UTC().Format(time.RFC3339),
		}
	}

	// We are sure this is a []byte if not nil
	b, _ := item.Value.([]byte)
	return &state.GetResponse{
		Data:     b,
		ETag:     ptr.Of(item.Etag),
		Metadata: metadata,
	}, nil
}

// getMulti retrieves multiple items with a cross-partition query, retrieving multiple records with a single query.
func (c *StateStore) getMulti(ctx context.Context, req []state.GetRequest) ([]state.BulkGetResponse, error) {
	// The partition key doesn't matter since it will be removed for a cross-partition query
	pk := azcosmos.NewPartitionKeyBool(true)

	// Build the query
	var consistency azcosmos.ConsistencyLevel
	keys := make([]string, len(req))
	for i, r := range req {
		keys[i] = r.Key

		// Use the specified consistency if empty
		// If there's a strong consistency, it overrides any eventual
		if r.Options.Consistency == state.Strong {
			consistency = azcosmos.ConsistencyLevelStrong
		} else if r.Options.Consistency == state.Eventual && consistency == "" {
			consistency = azcosmos.ConsistencyLevelEventual
		}
	}

	// Execute the query
	// Source for "ARRAY_CONTAINS": https://stackoverflow.com/a/44639998/192024
	queryOpts := &azcosmos.QueryOptions{
		QueryParameters: []azcosmos.QueryParameter{
			{Name: "@keys", Value: keys},
		},
	}
	if consistency != "" {
		queryOpts.ConsistencyLevel = &consistency
	}
	pager := c.client.NewQueryItemsPager(
		"SELECT * FROM r WHERE ARRAY_CONTAINS(@keys, r.id)",
		pk, queryOpts,
	)
	result := make([]state.BulkGetResponse, len(req))
	n := 0
	for pager.More() {
		queryResponse, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, value := range queryResponse.Items {
			item, err := NewCosmosItemFromResponse(value, c.logger)
			if err != nil {
				// If the error was while unserializing JSON, we don't have an ID, so exit right away
				// This should never happen, hopefully
				if item.ID == "" {
					return nil, err
				}
				result[n] = state.BulkGetResponse{
					Key:   item.ID,
					Error: err.Error(),
				}
				n++
				continue
			}

			var metadata map[string]string
			if item.TTL != nil {
				metadata = map[string]string{
					state.GetRespMetaKeyTTLExpireTime: time.Unix(item.TS+int64(*item.TTL), 0).UTC().Format(time.RFC3339),
				}
			}

			// We are sure this is a []byte if not nil
			b, _ := item.Value.([]byte)

			result[n] = state.BulkGetResponse{
				Key:      item.ID,
				Data:     b,
				ETag:     &item.Etag,
				Metadata: metadata,
			}
			n++
		}
	}

	return result[:n], nil
}

// BulkGet performs a Get operation in bulk.
func (c *StateStore) BulkGet(ctx context.Context, req []state.GetRequest, _ state.BulkGetOpts) ([]state.BulkGetResponse, error) {
	if len(req) == 0 {
		return []state.BulkGetResponse{}, nil
	}

	// If we have a single request, execute it as a regular Get request which is more efficient
	if len(req) == 1 {
		item, err := c.Get(ctx, &req[0])
		res := state.BulkGetResponse{
			Key: req[0].Key,
		}
		if err != nil {
			res.Error = err.Error()
		} else if item != nil {
			res.Data = json.RawMessage(item.Data)
			res.ETag = item.ETag
			res.Metadata = item.Metadata
		}
		return []state.BulkGetResponse{res}, nil
	}

	// Execute a single query to retrieve all values, as a cross-partition query
	items, err := c.getMulti(ctx, req)
	if err != nil {
		return nil, err
	}

	// Save all results
	// For consistency with when using Get, we need to include keys that are missing and set an empty value
	if len(items) > len(req) {
		return nil, errors.New("received more results than expected")
	}
	result := make([]state.BulkGetResponse, len(req))
	foundKeys := make(map[string]struct{}, len(items))
	n := 0
	for _, res := range items {
		result[n] = res
		foundKeys[res.Key] = struct{}{}
		n++
	}
	for _, r := range req {
		if _, ok := foundKeys[r.Key]; !ok {
			// Prevents a panic if out of bounds
			if n >= len(result) {
				return nil, errors.New("received more results than expected")
			}
			result[n] = state.BulkGetResponse{
				Key: r.Key,
			}
			n++
		}
	}
	// Prevents a panic if out of bounds
	if n > len(result) {
		n = len(result)
	}
	return result[:n], nil
}

// Set saves a CosmosDB item.
func (c *StateStore) Set(ctx context.Context, req *state.SetRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	partitionKey := populatePartitionMetadata(req.Key, req.Metadata)
	options := azcosmos.ItemOptions{}

	if req.HasETag() {
		etag := azcore.ETag(*req.ETag)
		options.IfMatchEtag = &etag
	} else if req.Options.Concurrency == state.FirstWrite {
		var u uuid.UUID
		u, err = uuid.NewRandom()
		if err != nil {
			return err
		}
		options.IfMatchEtag = ptr.Of(azcore.ETag(u.String()))
	}
	// Consistency levels can only be relaxed so the session level is used here
	if req.Options.Consistency == state.Strong {
		options.ConsistencyLevel = azcosmos.ConsistencyLevelSession.ToPtr()
	} else if req.Options.Consistency == state.Eventual {
		options.ConsistencyLevel = azcosmos.ConsistencyLevelEventual.ToPtr()
	}

	doc, err := createUpsertItem(c.contentType, *req, partitionKey)
	if err != nil {
		return err
	}

	marsh, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	upsertCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	pk := azcosmos.NewPartitionKeyString(partitionKey)
	_, err = c.client.UpsertItem(upsertCtx, pk, marsh, &options)
	if err != nil {
		resErr := &azcore.ResponseError{}
		if errors.As(err, &resErr) && resErr.StatusCode == http.StatusPreconditionFailed {
			return state.NewETagError(state.ETagMismatch, err)
		}
		return err
	}
	return nil
}

// Delete performs a delete operation.
func (c *StateStore) Delete(ctx context.Context, req *state.DeleteRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}
	partitionKey := populatePartitionMetadata(req.Key, req.Metadata)
	options := azcosmos.ItemOptions{}

	if req.HasETag() {
		etag := azcore.ETag(*req.ETag)
		options.IfMatchEtag = &etag
	} else if req.Options.Concurrency == state.FirstWrite {
		var u uuid.UUID
		u, err = uuid.NewRandom()
		if err != nil {
			return err
		}
		options.IfMatchEtag = ptr.Of(azcore.ETag(u.String()))
	}

	if req.Options.Consistency == state.Strong {
		options.ConsistencyLevel = azcosmos.ConsistencyLevelSession.ToPtr()
	} else if req.Options.Consistency == state.Eventual {
		options.ConsistencyLevel = azcosmos.ConsistencyLevelEventual.ToPtr()
	}

	deleteCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	pk := azcosmos.NewPartitionKeyString(partitionKey)
	_, err = c.client.DeleteItem(deleteCtx, pk, req.Key, &options)
	if err != nil && !isNotFoundError(err) {
		resErr := &azcore.ResponseError{}
		if errors.As(err, &resErr) && resErr.StatusCode == http.StatusPreconditionFailed {
			return state.NewETagError(state.ETagMismatch, err)
		}
		return err
	}

	return nil
}

// MultiMaxSize returns the maximum number of operations allowed in a transaction.
// For Azure Cosmos DB, that's 100.
func (c StateStore) MultiMaxSize() int {
	return 100
}

// Multi performs a transactional operation. Succeeds only if all operations succeed, and fails if one or more operations fail.
// Note that all operations must be in the same partition.
func (c *StateStore) Multi(ctx context.Context, request *state.TransactionalStateRequest) (err error) {
	if len(request.Operations) == 0 {
		c.logger.Debugf("No operations provided")
		return nil
	}

	partitionKey := request.Metadata[metadataPartitionKey]
	batch := c.client.NewTransactionalBatch(azcosmos.NewPartitionKeyString(partitionKey))

	numOperations := 0
	// Loop through the list of operations. Create and add the operation to the batch
	for _, o := range request.Operations {
		options := &azcosmos.TransactionalBatchItemOptions{}

		switch req := o.(type) {
		case state.SetRequest:
			var doc CosmosItem
			doc, err = createUpsertItem(c.contentType, req, partitionKey)
			if err != nil {
				return err
			}
			doc.PartitionKey = partitionKey

			if req.HasETag() {
				etag := azcore.ETag(*req.ETag)
				options.IfMatchETag = &etag
			} else if req.Options.Concurrency == state.FirstWrite {
				var u uuid.UUID
				u, err = uuid.NewRandom()
				if err != nil {
					return err
				}
				options.IfMatchETag = ptr.Of(azcore.ETag(u.String()))
			}

			var marsh []byte
			marsh, err = json.Marshal(doc)
			if err != nil {
				return err
			}
			batch.UpsertItem(marsh, options)
			numOperations++
		case state.DeleteRequest:
			if req.HasETag() {
				etag := azcore.ETag(*req.ETag)
				options.IfMatchETag = &etag
			} else if req.Options.Concurrency == state.FirstWrite {
				var u uuid.UUID
				u, err = uuid.NewRandom()
				if err != nil {
					return err
				}
				options.IfMatchETag = ptr.Of(azcore.ETag(u.String()))
			}

			batch.DeleteItem(req.Key, options)
			numOperations++
		}
	}

	c.logger.Debugf("#operations=%d,partitionkey=%s", numOperations, partitionKey)

	execCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	batchResponse, err := c.client.ExecuteTransactionalBatch(execCtx, batch, nil)
	if err != nil {
		return err
	}

	if !batchResponse.Success {
		// Transaction failed, look for the offending operation
		for index, operation := range batchResponse.OperationResults {
			if operation.StatusCode != http.StatusFailedDependency {
				c.logger.Errorf("Transaction failed due to operation %v which failed with status code %d", index, operation.StatusCode)
				return fmt.Errorf("transaction failed due to operation %v which failed with status code %d", index, operation.StatusCode)
			}
		}
		return errors.New("transaction failed")
	}

	// Transaction succeeded
	// We can inspect the individual operation results
	if c.logger.IsOutputLevelEnabled(logger.DebugLevel) {
		for index, operation := range batchResponse.OperationResults {
			c.logger.Debugf("Operation %v completed with status code %d", index, operation.StatusCode)
		}
	}

	return nil
}

func (c *StateStore) Query(ctx context.Context, req *state.QueryRequest) (*state.QueryResponse, error) {
	q := &Query{}

	qbuilder := query.NewQueryBuilder(q)
	if err := qbuilder.BuildQuery(&req.Query); err != nil {
		return &state.QueryResponse{}, err
	}

	// If present partitionKey, the value will be used in the query disabling the cross partition
	q.partitionKey = ""
	if val, found := req.Metadata[metadataPartitionKey]; found {
		q.partitionKey = val
	}

	data, token, err := q.execute(ctx, c.client)
	if err != nil {
		return nil, err
	}

	return &state.QueryResponse{
		Results: data,
		Token:   token,
	}, nil
}

func (c *StateStore) Ping(ctx context.Context) error {
	pingCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	_, err := c.client.Read(pingCtx, nil)
	if err != nil {
		return err
	}
	return nil
}

func (c *StateStore) Close() error {
	return nil
}

func createUpsertItem(contentType string, req state.SetRequest, partitionKey string) (CosmosItem, error) {
	byteArray, isBinary := req.Value.([]byte)
	if len(byteArray) == 0 {
		isBinary = false
	}

	ttl, err := stateutils.ParseTTL(req.Metadata)
	if err != nil {
		return CosmosItem{}, fmt.Errorf("error parsing TTL from metadata: %s", err)
	}

	if isBinary {
		if contenttype.IsJSONContentType(contentType) {
			var value map[string]interface{}
			err := json.Unmarshal(byteArray, &value)
			// if byte array is not a valid JSON, so keep it as-is to be Base64 encoded in CosmosDB.
			// otherwise, we save it as JSON
			if err == nil {
				item := CosmosItem{
					ID:           req.Key,
					Value:        value,
					PartitionKey: partitionKey,
					IsBinary:     false,
					TTL:          ttl,
				}
				return item, nil
			}
		} else if contenttype.IsStringContentType(contentType) {
			item := CosmosItem{
				ID:           req.Key,
				Value:        string(byteArray),
				PartitionKey: partitionKey,
				IsBinary:     false,
				TTL:          ttl,
			}
			return item, nil
		}
	}

	item := CosmosItem{
		ID:           req.Key,
		Value:        req.Value,
		PartitionKey: partitionKey,
		IsBinary:     isBinary,
		TTL:          ttl,
	}
	return item, nil
}

// This is a helper to return the partition key to use.  If if metadata["partitionkey"] is present,
// use that, otherwise use what's in "key".
func populatePartitionMetadata(key string, requestMetadata map[string]string) string {
	if val, found := requestMetadata[metadataPartitionKey]; found {
		return val
	}

	return key
}

func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	if requestError, ok := err.(*azcore.ResponseError); ok {
		if requestError.StatusCode == http.StatusNotFound {
			return true
		}
		// we previously checked the error code, but unfortunately this is not stable between API versions
	}

	return false
}

func NewCosmosItemFromResponse(value []byte, logger logger.Logger) (item CosmosItem, err error) {
	err = jsoniter.ConfigFastest.Unmarshal(value, &item)
	if err != nil {
		return item, err
	}

	if item.IsBinary {
		if item.Value == nil {
			return item, nil
		}

		strValue, ok := item.Value.(string)
		if !ok || strValue == "" {
			return item, nil
		}

		item.Value, err = base64.StdEncoding.DecodeString(strValue)
		if err != nil {
			logger.Warnf("CosmosDB state store Get request could not decode binary string: %v. Returning raw string instead.", err)
			item.Value = []byte(strValue)
		}

		return item, nil
	}

	item.Value, err = jsoniter.ConfigFastest.Marshal(&item.Value)
	if err != nil {
		return item, err
	}

	return item, nil
}
