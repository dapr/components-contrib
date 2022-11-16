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
	"strconv"
	"strings"
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
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

// StateStore is a CosmosDB state store.
type StateStore struct {
	state.DefaultBulkStore
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
	Etag         string
}

const (
	metadataPartitionKey = "partitionKey"
	metadataTTLKey       = "ttlInSeconds"
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
	s := &StateStore{
		logger: logger,
	}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)
	return s
}

func (c *StateStore) GetComponentMetadata() map[string]string {
	metadataStruct := metadata{}
	metadataInfo := map[string]string{}
	contribmeta.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo)
	return metadataInfo
}

// Init does metadata and connection parsing.
func (c *StateStore) Init(meta state.Metadata) error {
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
		env, err := azure.NewEnvironmentSettings("cosmosdb", meta.Properties)
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

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	_, err = c.client.Read(ctx, nil)
	cancel()
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
func (c *StateStore) Get(req *state.GetRequest) (*state.GetResponse, error) {
	partitionKey := populatePartitionMetadata(req.Key, req.Metadata)

	options := azcosmos.ItemOptions{}
	if req.Options.Consistency == state.Strong {
		options.ConsistencyLevel = azcosmos.ConsistencyLevelSession.ToPtr()
	} else if req.Options.Consistency == state.Eventual {
		options.ConsistencyLevel = azcosmos.ConsistencyLevelEventual.ToPtr()
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	readItem, err := c.client.ReadItem(ctx, azcosmos.NewPartitionKeyString(partitionKey), req.Key, &options)
	cancel()
	if err != nil {
		var responseErr *azcore.ResponseError
		if errors.As(err, &responseErr) && responseErr.ErrorCode == "NotFound" {
			return &state.GetResponse{}, nil
		}
		return nil, err
	}

	item := CosmosItem{}
	err = jsoniter.ConfigFastest.Unmarshal(readItem.Value, &item)
	if err != nil {
		return nil, err
	}

	item.Etag = string(readItem.Response.ETag)

	if item.IsBinary {
		if item.Value == nil {
			return &state.GetResponse{
				Data: make([]byte, 0),
				ETag: ptr.Of(item.Etag),
			}, nil
		}

		bytes, decodeErr := base64.StdEncoding.DecodeString(item.Value.(string))
		if decodeErr != nil {
			c.logger.Warnf("CosmosDB state store Get request could not decode binary string: %v. Returning raw string instead.", decodeErr)
			bytes = []byte(item.Value.(string))
		}

		return &state.GetResponse{
			Data: bytes,
			ETag: ptr.Of(item.Etag),
		}, nil
	}

	b, err := jsoniter.ConfigFastest.Marshal(&item.Value)
	if err != nil {
		return nil, err
	}

	return &state.GetResponse{
		Data: b,
		ETag: ptr.Of(item.Etag),
	}, nil
}

// Set saves a CosmosDB item.
func (c *StateStore) Set(req *state.SetRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	partitionKey := populatePartitionMetadata(req.Key, req.Metadata)
	options := azcosmos.ItemOptions{}

	if req.ETag != nil && *req.ETag != "" {
		etag := azcore.ETag(*req.ETag)
		options.IfMatchEtag = &etag
	}
	if req.Options.Concurrency == state.FirstWrite && (req.ETag == nil || *req.ETag == "") {
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

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	pk := azcosmos.NewPartitionKeyString(partitionKey)
	_, err = c.client.UpsertItem(ctx, pk, marsh, &options)
	cancel()
	if err != nil {
		return err
	}
	return nil
}

// Delete performs a delete operation.
func (c *StateStore) Delete(req *state.DeleteRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}
	partitionKey := populatePartitionMetadata(req.Key, req.Metadata)
	options := azcosmos.ItemOptions{}

	if req.ETag != nil && *req.ETag != "" {
		etag := azcore.ETag(*req.ETag)
		options.IfMatchEtag = &etag
	}
	if req.Options.Consistency == state.Strong {
		options.ConsistencyLevel = azcosmos.ConsistencyLevelSession.ToPtr()
	} else if req.Options.Consistency == state.Eventual {
		options.ConsistencyLevel = azcosmos.ConsistencyLevelEventual.ToPtr()
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	pk := azcosmos.NewPartitionKeyString(partitionKey)
	_, err = c.client.DeleteItem(ctx, pk, req.Key, &options)
	cancel()
	if err != nil && !isNotFoundError(err) {
		c.logger.Debugf("Error from cosmos.DeleteDocument e=%e, e.Error=%s", err, err.Error())
		if req.ETag != nil && *req.ETag != "" {
			return state.NewETagError(state.ETagMismatch, err)
		}
		return err
	}

	return nil
}

// Multi performs a transactional operation. succeeds only if all operations succeed, and fails if one or more operations fail.
func (c *StateStore) Multi(request *state.TransactionalStateRequest) (err error) {
	if len(request.Operations) == 0 {
		c.logger.Debugf("No Operations Provided")
		return nil
	}

	var partitionKey string
	partitionKey = populatePartitionMetadata(partitionKey, request.Metadata)

	batch := c.client.NewTransactionalBatch(azcosmos.NewPartitionKeyString(partitionKey))

	numOperations := 0
	// Loop through the list of operations. Create and add the operation to the batch
	for _, o := range request.Operations {
		var options *azcosmos.TransactionalBatchItemOptions

		if o.Operation == state.Upsert {
			req := o.Request.(state.SetRequest)
			var doc CosmosItem
			doc, err = createUpsertItem(c.contentType, req, partitionKey)
			if err != nil {
				return err
			}

			if req.ETag != nil && *req.ETag != "" {
				etag := azcore.ETag(*req.ETag)
				options.IfMatchETag = &etag
			}
			if req.Options.Concurrency == state.FirstWrite && (req.ETag == nil || *req.ETag == "") {
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
		} else if o.Operation == state.Delete {
			req := o.Request.(state.DeleteRequest)

			if req.ETag != nil && *req.ETag != "" {
				etag := azcore.ETag(*req.ETag)
				options.IfMatchETag = &etag
			}
			if req.Options.Concurrency == state.FirstWrite && (req.ETag == nil || *req.ETag == "") {
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

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	batchResponse, err := c.client.ExecuteTransactionalBatch(ctx, batch, nil)
	cancel()
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
	for index, operation := range batchResponse.OperationResults {
		c.logger.Debugf("Operation %v completed with status code %d", index, operation.StatusCode)
	}

	return nil
}

func (c *StateStore) Query(req *state.QueryRequest) (*state.QueryResponse, error) {
	q := &Query{}

	qbuilder := query.NewQueryBuilder(q)
	if err := qbuilder.BuildQuery(&req.Query); err != nil {
		return &state.QueryResponse{}, err
	}

	data, token, err := q.execute(c.client)
	if err != nil {
		return nil, err
	}

	return &state.QueryResponse{
		Results: data,
		Token:   token,
	}, nil
}

func (c *StateStore) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	_, err := c.client.Read(ctx, nil)
	cancel()
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

	ttl, err := parseTTL(req.Metadata)
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

func parseTTL(requestMetadata map[string]string) (*int, error) {
	if val, found := requestMetadata[metadataTTLKey]; found && val != "" {
		parsedVal, err := strconv.ParseInt(val, 10, 0)
		if err != nil {
			return nil, err
		}
		i := int(parsedVal)

		return &i, nil
	}

	return nil, nil
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
