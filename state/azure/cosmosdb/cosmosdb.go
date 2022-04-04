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
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/a8m/documentdb"
	"github.com/agrea/ptr"
	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"

	"github.com/dapr/components-contrib/authentication/azure"
	"github.com/dapr/components-contrib/contenttype"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/query"
	"github.com/dapr/kit/logger"
)

// Version of the stored procedure to use.
const spVersion = 2

//go:embed storedprocedures/__dapr_v2__.js
var spDefinition string

//go:embed storedprocedures/__daprver__.js
var spVersionDefinition string

// StateStore is a CosmosDB state store.
type StateStore struct {
	state.DefaultBulkStore
	client      *documentdb.DocumentDB
	metadata    metadata
	contentType string

	features []state.Feature
	logger   logger.Logger
}

type metadata struct {
	URL         string `json:"url"`
	MasterKey   string `json:"masterKey"`
	Database    string `json:"database"`
	Collection  string `json:"collection"`
	ContentType string `json:"contentType"`
}

type cosmosOperationType string

const (
	deleteOperationType cosmosOperationType = "delete"
	upsertOperationType cosmosOperationType = "upsert"
)

// CosmosOperation is a wrapper around a CosmosDB operation.
type CosmosOperation struct {
	Item CosmosItem          `json:"item"`
	Type cosmosOperationType `json:"type"`
}

// CosmosItem is a wrapper around a CosmosDB document.
type CosmosItem struct {
	documentdb.Document
	ID           string      `json:"id"`
	Value        interface{} `json:"value"`
	IsBinary     bool        `json:"isBinary"`
	PartitionKey string      `json:"partitionKey"`
	TTL          *int        `json:"ttl,omitempty"`
}

type storedProcedureDefinition struct {
	ID   string `json:"id"`
	Body string `json:"body"`
}

const (
	storedProcedureName   = "__dapr_v2__"
	versionSpName         = "__daprver__"
	metadataPartitionKey  = "partitionKey"
	unknownPartitionKey   = "__UNKNOWN__"
	metadataTTLKey        = "ttlInSeconds"
	statusTooManyRequests = "429" // RFC 6585, 4
	statusNotFound        = "NotFound"
)

// NewCosmosDBStateStore returns a new CosmosDB state store.
func NewCosmosDBStateStore(logger logger.Logger) *StateStore {
	s := &StateStore{
		features: []state.Feature{state.FeatureETag, state.FeatureTransactional},
		logger:   logger,
	}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
}

// Init does metadata and connection parsing.
func (c *StateStore) Init(meta state.Metadata) error {
	c.logger.Debugf("CosmosDB init start")

	connInfo := meta.Properties
	b, err := json.Marshal(connInfo)
	if err != nil {
		return err
	}

	m := metadata{
		ContentType: "application/json",
	}

	err = json.Unmarshal(b, &m)
	if err != nil {
		return err
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

	// Create the client; first, try authenticating with a master key, if present
	var config *documentdb.Config
	if m.MasterKey != "" {
		config = documentdb.NewConfig(&documentdb.Key{
			Key: m.MasterKey,
		})
	} else {
		// Fallback to using Azure AD
		env, errB := azure.NewEnvironmentSettings("cosmosdb", meta.Properties)
		if errB != nil {
			return errB
		}
		spt, errB := env.GetServicePrincipalToken()
		if errB != nil {
			return errB
		}
		config = documentdb.NewConfigWithServicePrincipal(spt)
	}
	config.WithAppIdentifier("dapr-" + logger.DaprVersion)

	c.client = documentdb.New(m.URL, config)
	c.metadata = m
	c.contentType = m.ContentType

	// Retries initializing the client if a TooManyRequests error is encountered
	err = retryOperation(func() (innerErr error) {
		_, innerErr = c.findCollection()
		if innerErr != nil {
			if isTooManyRequestsError(innerErr) {
				return innerErr
			}
			return backoff.Permanent(innerErr)
		}

		// if we're authenticating using Azure AD, we can't perform CRUD operations on stored procedures, so we need to try invoking the version SP and see if we get the desired version only
		if m.MasterKey == "" {
			innerErr = c.checkStoredProcedures()
		} else {
			innerErr = c.ensureStoredProcedures()
		}
		if innerErr != nil {
			if isTooManyRequestsError(innerErr) {
				return innerErr
			}
			return backoff.Permanent(innerErr)
		}

		return nil
	}, func(err error, d time.Duration) {
		c.logger.Warnf("CosmosDB state store initialization failed: %v; retrying in %s", err, d)
	}, 5*time.Minute)
	if err != nil {
		return err
	}

	c.logger.Debug("cosmos Init done")

	return nil
}

// Features returns the features available in this state store.
func (c *StateStore) Features() []state.Feature {
	return c.features
}

// Get retrieves a CosmosDB item.
func (c *StateStore) Get(req *state.GetRequest) (*state.GetResponse, error) {
	key := req.Key

	partitionKey := populatePartitionMetadata(req.Key, req.Metadata)
	items := []CosmosItem{}
	options := []documentdb.CallOption{documentdb.PartitionKey(partitionKey)}
	if req.Options.Consistency == state.Strong {
		options = append(options, documentdb.ConsistencyLevel(documentdb.Strong))
	}
	if req.Options.Consistency == state.Eventual {
		options = append(options, documentdb.ConsistencyLevel(documentdb.Eventual))
	}

	err := retryOperation(func() error {
		_, innerErr := c.client.QueryDocuments(
			c.getCollectionLink(),
			documentdb.NewQuery("SELECT * FROM ROOT r WHERE r.id=@id", documentdb.P{Name: "@id", Value: key}),
			&items,
			options...,
		)
		if innerErr != nil {
			if isTooManyRequestsError(innerErr) {
				return innerErr
			}
			return backoff.Permanent(innerErr)
		}
		return nil
	}, func(err error, d time.Duration) {
		c.logger.Warnf("CosmosDB state store Get request failed: %v; retrying in %s", err, d)
	}, 20*time.Second)
	if err != nil {
		return nil, err
	} else if len(items) == 0 {
		return &state.GetResponse{}, nil
	}

	if items[0].IsBinary {
		bytes, _ := base64.StdEncoding.DecodeString(items[0].Value.(string))

		return &state.GetResponse{
			Data: bytes,
			ETag: ptr.String(items[0].Etag),
		}, nil
	}

	b, err := jsoniter.ConfigFastest.Marshal(&items[0].Value)
	if err != nil {
		return nil, err
	}

	return &state.GetResponse{
		Data: b,
		ETag: ptr.String(items[0].Etag),
	}, nil
}

// Set saves a CosmosDB item.
func (c *StateStore) Set(req *state.SetRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	partitionKey := populatePartitionMetadata(req.Key, req.Metadata)
	options := []documentdb.CallOption{documentdb.PartitionKey(partitionKey)}

	if req.ETag != nil {
		options = append(options, documentdb.IfMatch((*req.ETag)))
	}
	if req.Options.Concurrency == state.FirstWrite && (req.ETag == nil || *req.ETag == "") {
		etag := uuid.NewString()
		options = append(options, documentdb.IfMatch((etag)))
	}
	if req.Options.Consistency == state.Strong {
		options = append(options, documentdb.ConsistencyLevel(documentdb.Strong))
	}
	if req.Options.Consistency == state.Eventual {
		options = append(options, documentdb.ConsistencyLevel(documentdb.Eventual))
	}

	doc, err := createUpsertItem(c.contentType, *req, partitionKey)
	if err != nil {
		return err
	}
	err = retryOperation(func() error {
		_, innerErr := c.client.UpsertDocument(c.getCollectionLink(), &doc, options...)
		if innerErr != nil {
			if isTooManyRequestsError(innerErr) {
				return innerErr
			}
			return backoff.Permanent(innerErr)
		}
		return nil
	}, func(err error, d time.Duration) {
		c.logger.Warnf("CosmosDB state store Set request failed: %v; retrying in %s", err, d)
	}, 20*time.Second)

	if err != nil {
		if req.ETag != nil {
			return state.NewETagError(state.ETagMismatch, err)
		}

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
	options := []documentdb.CallOption{documentdb.PartitionKey(partitionKey)}

	items := []CosmosItem{}
	_, err = c.client.QueryDocuments(
		c.getCollectionLink(),
		documentdb.NewQuery("SELECT * FROM ROOT r WHERE r.id=@id", documentdb.P{Name: "@id", Value: req.Key}),
		&items,
		options...,
	)
	if err != nil {
		return err
	} else if len(items) == 0 {
		return nil
	}

	if req.ETag != nil {
		options = append(options, documentdb.IfMatch((*req.ETag)))
	}
	if req.Options.Consistency == state.Strong {
		options = append(options, documentdb.ConsistencyLevel(documentdb.Strong))
	}
	if req.Options.Consistency == state.Eventual {
		options = append(options, documentdb.ConsistencyLevel(documentdb.Eventual))
	}

	err = retryOperation(func() error {
		_, innerErr := c.client.DeleteDocument(items[0].Self, options...)
		if innerErr != nil {
			if isTooManyRequestsError(innerErr) {
				return innerErr
			}
			return backoff.Permanent(innerErr)
		}
		return nil
	}, func(err error, d time.Duration) {
		c.logger.Warnf("CosmosDB state store Delete request failed: %v; retrying in %s", err, d)
	}, 20*time.Second)

	if err != nil {
		c.logger.Debugf("Error from cosmos.DeleteDocument e=%e, e.Error=%s", err, err.Error())
		if req.ETag != nil {
			return state.NewETagError(state.ETagMismatch, err)
		}
	}

	return err
}

// Multi performs a transactional operation. succeeds only if all operations succeed, and fails if one or more operations fail.
func (c *StateStore) Multi(request *state.TransactionalStateRequest) error {
	operations := []CosmosOperation{}

	partitionKey := unknownPartitionKey

	for _, o := range request.Operations {
		t := o.Request.(state.KeyInt)
		key := t.GetKey()

		partitionKey = populatePartitionMetadata(key, request.Metadata)
		if o.Operation == state.Upsert {
			req := o.Request.(state.SetRequest)

			item, err := createUpsertItem(c.contentType, req, partitionKey)
			if err != nil {
				return err
			}
			upsertOperation := CosmosOperation{
				Item: item,
				Type: upsertOperationType,
			}
			operations = append(operations, upsertOperation)
		} else if o.Operation == state.Delete {
			req := o.Request.(state.DeleteRequest)

			deleteOperation := CosmosOperation{
				Item: CosmosItem{
					ID:           req.Key,
					Value:        "", // Value does not need to be specified
					PartitionKey: partitionKey,
				},
				Type: deleteOperationType,
			}
			operations = append(operations, deleteOperation)
		}
	}

	c.logger.Debugf("#operations=%d,partitionkey=%s", len(operations), partitionKey)

	var retString string

	// The stored procedure throws if it failed, which sets err to non-nil.  It doesn't return anything else.
	err := retryOperation(func() error {
		innerErr := c.client.ExecuteStoredProcedure(
			c.getSprocLink(storedProcedureName),
			[...]interface{}{operations},
			&retString,
			documentdb.PartitionKey(partitionKey),
		)
		if innerErr != nil {
			if isTooManyRequestsError(innerErr) {
				return innerErr
			}
			return backoff.Permanent(innerErr)
		}
		return nil
	}, func(err error, d time.Duration) {
		c.logger.Warnf("CosmosDB state store Multi request failed: %v; retrying in %s", err, d)
	}, 20*time.Second)
	if err != nil {
		c.logger.Debugf("error=%e", err)
		return err
	}

	return nil
}

func (c *StateStore) Query(req *state.QueryRequest) (*state.QueryResponse, error) {
	q := &Query{}
	qbuilder := query.NewQueryBuilder(q)
	if err := qbuilder.BuildQuery(&req.Query); err != nil {
		return &state.QueryResponse{}, err
	}
	var data []state.QueryItem
	var token string
	err := retryOperation(func() error {
		var innerErr error
		data, token, innerErr = q.execute(c.client, c.getCollectionLink())
		if innerErr != nil {
			if isTooManyRequestsError(innerErr) {
				return innerErr
			}
			return backoff.Permanent(innerErr)
		}
		return nil
	}, func(err error, d time.Duration) {
		c.logger.Warnf("CosmosDB state store Ping request failed: %v; retrying in %s", err, d)
	}, 20*time.Second)
	if err != nil {
		return &state.QueryResponse{}, err
	}

	return &state.QueryResponse{
		Results: data,
		Token:   token,
	}, nil
}

func (c *StateStore) Ping() error {
	return retryOperation(func() error {
		_, innerErr := c.findCollection()
		if innerErr != nil {
			if isTooManyRequestsError(innerErr) {
				return innerErr
			}
			return backoff.Permanent(innerErr)
		}
		return nil
	}, func(err error, d time.Duration) {
		c.logger.Warnf("CosmosDB state store Ping request failed: %v; retrying in %s", err, d)
	}, 20*time.Second)
}

// getCollectionLink returns the link to the collection.
func (c *StateStore) getCollectionLink() string {
	return fmt.Sprintf("dbs/%s/colls/%s/", c.metadata.Database, c.metadata.Collection)
}

// getSprocLink returns the link to the stored procedures.
func (c *StateStore) getSprocLink(sprocName string) string {
	return fmt.Sprintf("dbs/%s/colls/%s/sprocs/%s", c.metadata.Database, c.metadata.Collection, sprocName)
}

func (c *StateStore) checkStoredProcedures() error {
	var ver int
	err := c.client.ExecuteStoredProcedure(c.getSprocLink(versionSpName), nil, &ver, documentdb.PartitionKey("1"))
	if err == nil {
		c.logger.Debugf("Cosmos DB stored procedure version: %d", ver)
	}
	if err != nil || (err == nil && ver != spVersion) {
		return fmt.Errorf("Dapr requires stored procedures created in Cosmos DB before it can be used as state store. Those stored procedures are currently not existing or are using a different version than expected. When you authenticate using Azure AD we cannot automatically create them for you: please start this state store with a Cosmos DB master key just once so we can create the stored procedures for you; otherwise, you can check our docs to learn how to create them yourself: https://aka.ms/dapr/cosmosdb-aad") // nolint:stylecheck
	}
	return nil
}

func (c *StateStore) ensureStoredProcedures() error {
	spLink := c.getSprocLink(storedProcedureName)
	verSpLink := c.getSprocLink(versionSpName)

	// get a link to the sp's
	sp, err := c.client.ReadStoredProcedure(spLink)
	if err != nil && !isNotFoundError(err) {
		return err
	}
	verSp, err := c.client.ReadStoredProcedure(verSpLink)
	if err != nil && !isNotFoundError(err) {
		return err
	}

	// check version
	replace := false
	if verSp != nil {
		var ver int
		err = c.client.ExecuteStoredProcedure(verSpLink, nil, &ver, documentdb.PartitionKey("1"))
		if err == nil {
			c.logger.Debugf("Cosmos DB stored procedure version: %d", ver)
		}
		if err != nil || (err == nil && ver != spVersion) {
			// ignore errors: just replace the stored procedures
			replace = true
		}
	}
	if verSp == nil || replace {
		// register/replace the stored procedure
		createspBody := storedProcedureDefinition{ID: versionSpName, Body: spVersionDefinition}
		if replace && verSp != nil {
			c.logger.Debugf("Replacing Cosmos DB stored procedure %s", versionSpName)
			_, err = c.client.ReplaceStoredProcedure(verSp.Self, createspBody)
		} else {
			c.logger.Debugf("Creating Cosmos DB stored procedure %s", versionSpName)
			_, err = c.client.CreateStoredProcedure(c.getCollectionLink(), createspBody)
		}
		// if it already exists that is success (Conflict should only happen on Create commands)
		if err != nil && !strings.HasPrefix(err.Error(), "Conflict") {
			return err
		}
	}

	if sp == nil || replace {
		// register the stored procedure
		createspBody := storedProcedureDefinition{ID: storedProcedureName, Body: spDefinition}
		if replace && sp != nil {
			c.logger.Debugf("Replacing Cosmos DB stored procedure %s", storedProcedureName)
			_, err = c.client.ReplaceStoredProcedure(sp.Self, createspBody)
		} else {
			c.logger.Debugf("Creating Cosmos DB stored procedure %s", storedProcedureName)
			_, err = c.client.CreateStoredProcedure(c.getCollectionLink(), createspBody)
		}
		// if it already exists that is success (Conflict should only happen on Create commands)
		if err != nil && !strings.HasPrefix(err.Error(), "Conflict") {
			return err
		}
	}

	return nil
}

func (c *StateStore) findCollection() (*documentdb.Collection, error) {
	coll, err := c.client.ReadCollection(c.getCollectionLink())
	if err != nil {
		return nil, err
	}
	if coll == nil || coll.Self == "" {
		return nil, fmt.Errorf("collection %s in database %s for CosmosDB state store not found. This must be created before Dapr uses it", c.metadata.Collection, c.metadata.Database)
	}
	return coll, nil
}

func createUpsertItem(contentType string, req state.SetRequest, partitionKey string) (CosmosItem, error) {
	byteArray, isBinary := req.Value.([]uint8)

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
				return CosmosItem{
					ID:           req.Key,
					Value:        value,
					PartitionKey: partitionKey,
					IsBinary:     false,
					TTL:          ttl,
				}, nil
			}
		} else if contenttype.IsStringContentType(contentType) {
			return CosmosItem{
				ID:           req.Key,
				Value:        string(byteArray),
				PartitionKey: partitionKey,
				IsBinary:     false,
				TTL:          ttl,
			}, nil
		}
	}

	return CosmosItem{
		ID:           req.Key,
		Value:        req.Value,
		PartitionKey: partitionKey,
		IsBinary:     isBinary,
		TTL:          ttl,
	}, nil
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

func retryOperation(operation backoff.Operation, notify backoff.Notify, maxElapsedTime time.Duration) error {
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 2 * time.Second
	bo.MaxElapsedTime = maxElapsedTime
	return backoff.RetryNotify(operation, bo, notify)
}

func isTooManyRequestsError(err error) bool {
	if err == nil {
		return false
	}

	if requestError, ok := err.(*documentdb.RequestError); ok {
		if requestError.Code == statusTooManyRequests {
			return true
		}
	}

	return false
}

func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	if requestError, ok := err.(*documentdb.RequestError); ok {
		if requestError.Code == statusNotFound {
			return true
		}
	}

	return false
}
