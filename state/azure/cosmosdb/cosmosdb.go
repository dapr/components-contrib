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
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"

	"github.com/dapr/components-contrib/contenttype"
	"github.com/dapr/components-contrib/internal/authentication/azure"
	contribmeta "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/query"
	stateutils "github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

// StateStore is a CosmosDB state store.
type StateStore struct {
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
}

const (
	metadataPartitionKey = "partitionKey"
	defaultTimeout       = 20 * time.Second
	statusNotFound       = "NotFound"
)

// policy that tracks the number of times it was invoked
type crossPartitionQueryPolicy struct{}

func (p *crossPartitionQueryPolicy) Do(req *policy.Request) (*http.Response, error) {
	raw := req.Raw()
	hdr := raw.Header
	if strings.ToLower(hdr.Get("x-ms-documentdb-query")) == "true" {
		// modify req here since we know it is a query
		hdr.Add("x-ms-documentdb-query-enablecrosspartition", "true")
		hdr.Del("x-ms-documentdb-partitionkey")
		raw.Header = hdr
	}
	return req.Next()
}

// NewCosmosDBStateStore returns a new CosmosDB state store.
func NewCosmosDBStateStore(logger logger.Logger) state.Store {
	return &StateStore{
		logger: logger,
	}
}

func (c *StateStore) GetComponentMetadata() map[string]string {
	metadataStruct := metadata{}
	metadataInfo := map[string]string{}
	contribmeta.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo)
	return metadataInfo
}

// Init does metadata and connection parsing.
func (c *StateStore) Init(ctx context.Context, meta state.Metadata) error {
	c.logger.Debugf("CosmosDB init start")

	m := metadata{
		ContentType: "application/json",
	}
	errDecode := contribmeta.DecodeMetadata(meta.Properties, &m)
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
	queryPolicy := &crossPartitionQueryPolicy{}
	opts := azcosmos.ClientOptions{
		ClientOptions: policy.ClientOptions{
			PerCallPolicies: []policy.Policy{queryPolicy},
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
		var responseErr *azcore.ResponseError
		if errors.As(err, &responseErr) && responseErr.ErrorCode == "NotFound" {
			return &state.GetResponse{}, nil
		}
		return nil, err
	}

	item, err := NewCosmosItemFromResponse(readItem.Value, c.logger)
	if err != nil {
		return nil, err
	}

	// We are sure this is a []byte if not nil
	b, _ := item.Value.([]byte)
	return &state.GetResponse{
		Data: b,
		ETag: ptr.Of(item.Etag),
	}, nil
}

// getMulti retrieves multiple items within a single partition, efficiently with a single query.
func (c *StateStore) getMulti(ctx context.Context, pk azcosmos.PartitionKey, req []*state.GetRequest) ([]state.BulkGetResponse, error) {
	if len(req) == 0 {
		return []state.BulkGetResponse{}, nil
	}

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
			{Name: "keys", Value: keys},
		},
	}
	if consistency != "" {
		queryOpts.ConsistencyLevel = &consistency
	}
	pager := c.client.NewQueryItemsPager("SELECT * FROM r WHERE ARRAY_CONTAINS(@keys, r.id)", pk, queryOpts)
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

			// We are sure this is a []byte if not nil
			b, _ := item.Value.([]byte)

			result[n] = state.BulkGetResponse{
				Key:  item.ID,
				Data: b,
				ETag: &item.Etag,
			}
			n++
		}
	}

	return result[:n], nil
}

// BulkGet performs a Get operation in bulk.
func (c *StateStore) BulkGet(ctx context.Context, req []state.GetRequest, opts state.BulkGetOpts) ([]state.BulkGetResponse, error) {
	// If parallelism isn't set, run all operations in parallel
	if opts.Parallelism <= 0 {
		opts.Parallelism = len(req)
	}

	// Group all requests by partition key
	// This has initial size of len(req) as pessimistic scenario
	partitions := make(map[string][]*state.GetRequest, len(req))
	for i := range req {
		r := req[i]
		pk := populatePartitionMetadata(r.GetKey(), r.GetMetadata())
		if partitions[pk] == nil {
			partitions[pk] = make([]*state.GetRequest, 0, 1)
		}

		partitions[pk] = append(partitions[pk], &r)
	}

	// Execute all transactions in parallel, with max parallelism
	limitCh := make(chan struct{}, opts.Parallelism)
	result := make([]state.BulkGetResponse, len(req))
	resN := atomic.Int64{}
	for pk := range partitions {
		// Limit concurrency
		limitCh <- struct{}{}

		// If there's only 1 item in the partition, use Get()
		// This performs a lookup by key so it's more efficient inside Cosmos DB
		if len(partitions[pk]) == 1 {
			r := partitions[pk][0]
			go func() {
				item, err := c.Get(ctx, r)
				res := state.BulkGetResponse{
					Key: r.Key,
				}
				if err != nil {
					res.Error = err.Error()
				} else if item != nil {
					res.Data = json.RawMessage(item.Data)
					res.ETag = item.ETag
					res.Metadata = item.Metadata
				}

				// Release the token for concurrency and store the response
				<-limitCh
				result[int(resN.Add(1)-1)] = res
			}()

			continue
		}

		// With more than 1 item, use getMulti
		go func(pk string) {
			defer func() {
				// Release the token for concurrency
				<-limitCh
			}()

			items, err := c.getMulti(ctx, azcosmos.NewPartitionKeyString(pk), partitions[pk])
			if err != nil {
				c.logger.Errorf("Error while requesting values in partition %s: %w", pk, err)
				return
			}

			// Save all results
			// We must do this so we can be safe with other goroutines
			for _, res := range items {
				result[int(resN.Add(1)-1)] = res
			}
		}(pk)
	}

	// We can detect that all goroutines are done when limitCh is completely empty
	for i := 0; i < opts.Parallelism; i++ {
		limitCh <- struct{}{}
	}

	return result[:int(resN.Load())], nil
}

// Set saves a CosmosDB item.
func (c *StateStore) Set(ctx context.Context, req *state.SetRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	partitionKey := populatePartitionMetadata(req.Key, req.Metadata)
	options := azcosmos.ItemOptions{}

	if req.ETag != nil && *req.ETag != "" {
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
		return err
	}
	return nil
}

// BulkSet performs a bulk save operation.
func (c *StateStore) BulkSet(ctx context.Context, req []state.SetRequest) error {
	return c.doBulkSetDelete(ctx, state.ToTransactionalStateOperationSlice(req))
}

// Delete performs a delete operation.
func (c *StateStore) Delete(ctx context.Context, req *state.DeleteRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}
	partitionKey := populatePartitionMetadata(req.Key, req.Metadata)
	options := azcosmos.ItemOptions{}

	if req.ETag != nil && *req.ETag != "" {
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
		if req.ETag != nil && *req.ETag != "" {
			return state.NewETagError(state.ETagMismatch, err)
		}
		return err
	}

	return nil
}

// BulkDelete performs a bulk delete operation.
func (c *StateStore) BulkDelete(ctx context.Context, req []state.DeleteRequest) error {
	return c.doBulkSetDelete(ctx, state.ToTransactionalStateOperationSlice(req))
}

// Custom implementation for BulkSet and BulkDelete. We can't use the standard in DefaultBulkStore because we need to be aware of partition keys.
func (c *StateStore) doBulkSetDelete(ctx context.Context, req []state.TransactionalStateOperation) error {
	// Group all requests by partition key
	// This has initial size of len(req) as pessimistic scenario
	partitions := make(map[string]*state.TransactionalStateRequest, len(req))
	for i := range req {
		r := req[i]
		pk := populatePartitionMetadata(r.GetKey(), r.GetMetadata())
		if partitions[pk] == nil {
			partitions[pk] = &state.TransactionalStateRequest{
				Operations: make([]state.TransactionalStateOperation, 0),
				Metadata: map[string]string{
					metadataPartitionKey: pk,
				},
			}
		}

		partitions[pk].Operations = append(partitions[pk].Operations, r)
	}

	// Execute all transactions in parallel
	errs := make(chan error)
	for pk := range partitions {
		go func(pk string) {
			err := c.Multi(ctx, partitions[pk])
			if err != nil {
				err = fmt.Errorf("error while processing bulk set in partition %s: %w", pk, err)
			}
			errs <- err
		}(pk)
	}

	// Wait for all results
	var err error
	for i := 0; i < len(partitions); i++ {
		err = errors.Join(<-errs)
	}

	return err
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

			if req.ETag != nil && *req.ETag != "" {
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
			batch.UpsertItem(marsh, nil)
			numOperations++
		case state.DeleteRequest:
			if req.ETag != nil && *req.ETag != "" {
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
		if requestError.ErrorCode == statusNotFound {
			return true
		}
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
