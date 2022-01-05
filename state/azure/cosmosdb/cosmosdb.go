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

// StateStore is a CosmosDB state store.
type StateStore struct {
	state.DefaultBulkStore
	client      *documentdb.DocumentDB
	collection  *documentdb.Collection
	db          *documentdb.Database
	sp          *documentdb.Sproc
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
	storedProcedureName   = "__dapr__"
	metadataPartitionKey  = "partitionKey"
	unknownPartitionKey   = "__UNKNOWN__"
	metadataTTLKey        = "ttlInSeconds"
	statusTooManyRequests = "429" // RFC 6585, 4
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

	// Retries initializing the client if a TooManyRequests error is encountered
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 2 * time.Second
	bo.MaxElapsedTime = 5 * time.Minute
	err = backoff.RetryNotify(func() (err error) {
		client := documentdb.New(m.URL, config)

		dbs, err := client.QueryDatabases(&documentdb.Query{
			Query: "SELECT * FROM ROOT r WHERE r.id=@id",
			Parameters: []documentdb.Parameter{
				{Name: "@id", Value: m.Database},
			},
		})

		if err != nil {
			if isTooManyRequestsError(err) {
				return err
			}
			return backoff.Permanent(err)
		} else if len(dbs) == 0 {
			return backoff.Permanent(fmt.Errorf("database %s for CosmosDB state store not found", m.Database))
		}

		c.db = &dbs[0]
		colls, err := client.QueryCollections(c.db.Self, &documentdb.Query{
			Query: "SELECT * FROM ROOT r WHERE r.id=@id",
			Parameters: []documentdb.Parameter{
				{Name: "@id", Value: m.Collection},
			},
		})
		if err != nil {
			if isTooManyRequestsError(err) {
				return err
			}
			return backoff.Permanent(err)
		} else if len(colls) == 0 {
			return backoff.Permanent(fmt.Errorf("collection %s for CosmosDB state store not found.  This must be created before Dapr uses it", m.Collection))
		}

		c.metadata = m
		c.collection = &colls[0]
		c.client = client
		c.contentType = m.ContentType

		sps, err := c.client.ReadStoredProcedures(c.collection.Self)
		if err != nil {
			if isTooManyRequestsError(err) {
				return err
			}
			return backoff.Permanent(err)
		}

		// get a link to the sp
		for i := range sps {
			if sps[i].Id == storedProcedureName {
				c.sp = &sps[i]

				break
			}
		}

		if c.sp == nil {
			// register the stored procedure
			createspBody := storedProcedureDefinition{ID: storedProcedureName, Body: spDefinition}
			c.sp, err = c.client.CreateStoredProcedure(c.collection.Self, createspBody)
			if err != nil {
				if isTooManyRequestsError(err) {
					return err
				}
				// if it already exists that is success
				if !strings.HasPrefix(err.Error(), "Conflict") {
					return backoff.Permanent(err)
				}
			}
		}

		return nil
	}, bo, func(err error, d time.Duration) {
		c.logger.Warnf("CosmosDB state store initialization failed: %v; retrying in %s", err, d)
	})
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

	_, err := c.client.QueryDocuments(
		c.collection.Self,
		documentdb.NewQuery("SELECT * FROM ROOT r WHERE r.id=@id", documentdb.P{Name: "@id", Value: key}),
		&items,
		options...,
	)
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
	_, err = c.client.UpsertDocument(c.collection.Self, &doc, options...)

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
		c.collection.Self,
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

	_, err = c.client.DeleteDocument(items[0].Self, options...)
	if err != nil {
		c.logger.Debugf("Error from cosmos.DeleteDocument e=%e, e.Error=%s", err, err.Error())
	}

	if err != nil {
		if req.ETag != nil {
			return state.NewETagError(state.ETagMismatch, err)
		}
	}

	return err
}

// Multi performs a transactional operation. succeeds only if all operations succeed, and fails if one or more operations fail.
func (c *StateStore) Multi(request *state.TransactionalStateRequest) error {
	upserts := []CosmosItem{}
	deletes := []CosmosItem{}

	partitionKey := unknownPartitionKey

	for _, o := range request.Operations {
		t := o.Request.(state.KeyInt)
		key := t.GetKey()

		partitionKey = populatePartitionMetadata(key, request.Metadata)
		if o.Operation == state.Upsert {
			req := o.Request.(state.SetRequest)

			upsertOperation, err := createUpsertItem(c.contentType, req, partitionKey)
			if err != nil {
				return err
			}
			upserts = append(upserts, upsertOperation)
		} else if o.Operation == state.Delete {
			req := o.Request.(state.DeleteRequest)

			deleteOperation := CosmosItem{
				ID:           req.Key,
				Value:        "", // Value does not need to be specified
				PartitionKey: partitionKey,
			}
			deletes = append(deletes, deleteOperation)
		}
	}

	c.logger.Debugf("#upserts=%d,#deletes=%d, partitionkey=%s", len(upserts), len(deletes), partitionKey)

	options := []documentdb.CallOption{documentdb.PartitionKey(partitionKey)}
	var retString string

	// The stored procedure throws if it failed, which sets err to non-nil.  It doesn't return anything else.
	err := c.client.ExecuteStoredProcedure(c.sp.Self, [...]interface{}{upserts, deletes}, &retString, options...)
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
	data, token, err := q.execute(c.client, c.collection)
	if err != nil {
		return &state.QueryResponse{}, err
	}

	return &state.QueryResponse{
		Results: data,
		Token:   token,
	}, nil
}

func (c *StateStore) Ping() error {
	m := c.metadata

	colls, err := c.client.QueryCollections(c.db.Self, &documentdb.Query{
		Query: "SELECT * FROM ROOT r WHERE r.id=@id",
		Parameters: []documentdb.Parameter{
			{Name: "@id", Value: m.Collection},
		},
	})
	if err != nil {
		return err
	} else if len(colls) == 0 {
		return fmt.Errorf("collection %s for CosmosDB state store not found", m.Collection)
	}

	return nil
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
