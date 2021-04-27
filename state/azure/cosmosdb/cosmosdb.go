// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package cosmosdb

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/a8m/documentdb"
	"github.com/agrea/ptr"
	jsoniter "github.com/json-iterator/go"

	"github.com/dapr/components-contrib/contenttype"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

// StateStore is a CosmosDB state store
type StateStore struct {
	state.DefaultBulkStore
	client      *documentdb.DocumentDB
	collection  *documentdb.Collection
	db          *documentdb.Database
	sp          *documentdb.Sproc
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

// CosmosItem is a wrapper around a CosmosDB document
type CosmosItem struct {
	documentdb.Document
	ID           string      `json:"id"`
	Value        interface{} `json:"value"`
	IsBinary     bool        `json:"isBinary"`
	PartitionKey string      `json:"partitionKey"`
}

type storedProcedureDefinition struct {
	ID   string `json:"id"`
	Body string `json:"body"`
}

const (
	storedProcedureName  = "__dapr__"
	metadataPartitionKey = "partitionKey"
	unknownPartitionKey  = "__UNKNOWN__"
)

// NewCosmosDBStateStore returns a new CosmosDB state store
func NewCosmosDBStateStore(logger logger.Logger) *StateStore {
	s := &StateStore{
		features: []state.Feature{state.FeatureETag, state.FeatureTransactional},
		logger:   logger,
	}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
}

// Init does metadata and connection parsing
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
	if m.MasterKey == "" {
		return errors.New("masterKey is required")
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

	client := documentdb.New(m.URL, &documentdb.Config{
		MasterKey: &documentdb.Key{
			Key: m.MasterKey,
		},
	})

	dbs, err := client.QueryDatabases(&documentdb.Query{
		Query: "SELECT * FROM ROOT r WHERE r.id=@id",
		Parameters: []documentdb.Parameter{
			{Name: "@id", Value: m.Database},
		},
	})
	if err != nil {
		return err
	} else if len(dbs) == 0 {
		return fmt.Errorf("database %s for CosmosDB state store not found", m.Database)
	}

	c.db = &dbs[0]
	colls, err := client.QueryCollections(c.db.Self, &documentdb.Query{
		Query: "SELECT * FROM ROOT r WHERE r.id=@id",
		Parameters: []documentdb.Parameter{
			{Name: "@id", Value: m.Collection},
		},
	})
	if err != nil {
		return err
	} else if len(colls) == 0 {
		return fmt.Errorf("collection %s for CosmosDB state store not found.  This must be created before Dapr uses it", m.Collection)
	}

	c.collection = &colls[0]
	c.client = client
	c.contentType = m.ContentType

	sps, err := c.client.ReadStoredProcedures(c.collection.Self)
	if err != nil {
		return err
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
			// if it already exists that is success
			if !strings.HasPrefix(err.Error(), "Conflict") {
				return err
			}
		}
	}

	c.logger.Debug("cosmos Init done")

	return nil
}

// Features returns the features available in this state store
func (c *StateStore) Features() []state.Feature {
	return c.features
}

// Get retrieves a CosmosDB item
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

// Set saves a CosmosDB item
func (c *StateStore) Set(req *state.SetRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	partitionKey := populatePartitionMetadata(req.Key, req.Metadata)
	options := []documentdb.CallOption{documentdb.PartitionKey(partitionKey)}

	if req.ETag != nil {
		var etag string
		if req.ETag != nil {
			etag = *req.ETag
		}
		options = append(options, documentdb.IfMatch((etag)))
	}
	if req.Options.Consistency == state.Strong {
		options = append(options, documentdb.ConsistencyLevel(documentdb.Strong))
	}
	if req.Options.Consistency == state.Eventual {
		options = append(options, documentdb.ConsistencyLevel(documentdb.Eventual))
	}

	doc := createUpsertItem(c.contentType, *req, partitionKey)
	_, err = c.client.UpsertDocument(c.collection.Self, doc, options...)

	if err != nil {
		if req.ETag != nil {
			return state.NewETagError(state.ETagMismatch, err)
		}

		return err
	}

	return nil
}

// Delete performs a delete operation
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
		var etag string
		if req.ETag != nil {
			etag = *req.ETag
		}
		options = append(options, documentdb.IfMatch((etag)))
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

// Multi performs a transactional operation. succeeds only if all operations succeed, and fails if one or more operations fail
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

			upsertOperation := createUpsertItem(c.contentType, req, partitionKey)
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

func createUpsertItem(contentType string, req state.SetRequest, partitionKey string) CosmosItem {
	byteArray, isBinary := req.Value.([]uint8)
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
				}
			}
		} else if contenttype.IsStringContentType(contentType) {
			return CosmosItem{
				ID:           req.Key,
				Value:        string(byteArray),
				PartitionKey: partitionKey,
				IsBinary:     false,
			}
		}
	}

	return CosmosItem{
		ID:           req.Key,
		Value:        req.Value,
		PartitionKey: partitionKey,
		IsBinary:     isBinary,
	}
}

// This is a helper to return the partition key to use.  If if metadata["partitionkey"] is present,
// use that, otherwise use what's in "key".
func populatePartitionMetadata(key string, requestMetadata map[string]string) string {
	if val, found := requestMetadata[metadataPartitionKey]; found {
		return val
	}

	return key
}
