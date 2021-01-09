// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package cosmosdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/a8m/documentdb"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/dapr/pkg/logger"
	jsoniter "github.com/json-iterator/go"
)

// StateStore is a CosmosDB state store
type StateStore struct {
	state.DefaultBulkStore
	client     *documentdb.DocumentDB
	collection *documentdb.Collection
	db         *documentdb.Database
	sp         *documentdb.Sproc

	logger logger.Logger
}

type credentials struct {
	URL        string `json:"url"`
	MasterKey  string `json:"masterKey"`
	Database   string `json:"database"`
	Collection string `json:"collection"`
}

// CosmosItem is a wrapper around a CosmosDB document
type CosmosItem struct {
	documentdb.Document
	ID           string      `json:"id"`
	Value        interface{} `json:"value"`
	PartitionKey string      `json:"partitionKey"`
}

// CosmosItemWithRawMessage is a version of CosmosItem with a Value of RawMessage so this field
// is not marshalled.  If it is marshalled it will end up in a form that
// cannot be unmarshalled.  This type is used when SetRequest.Value arrives as bytes.
type CosmosItemWithRawMessage struct {
	documentdb.Document
	ID           string              `json:"id"`
	Value        jsoniter.RawMessage `json:"value"`
	PartitionKey string              `json:"partitionKey"`
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
	s := &StateStore{logger: logger}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
}

// Init does metadata and connection parsing
func (c *StateStore) Init(metadata state.Metadata) error {
	c.logger.Debugf("CosmosDB init start")

	connInfo := metadata.Properties
	b, err := json.Marshal(connInfo)
	if err != nil {
		return err
	}

	var creds credentials
	err = json.Unmarshal(b, &creds)
	if err != nil {
		return err
	}

	client := documentdb.New(creds.URL, &documentdb.Config{
		MasterKey: &documentdb.Key{
			Key: creds.MasterKey,
		},
	})

	dbs, err := client.QueryDatabases(&documentdb.Query{
		Query: "SELECT * FROM ROOT r WHERE r.id=@id",
		Parameters: []documentdb.Parameter{
			{Name: "@id", Value: creds.Database},
		},
	})
	if err != nil {
		return err
	} else if len(dbs) == 0 {
		return fmt.Errorf("database %s for CosmosDB state store not found", creds.Database)
	}

	c.db = &dbs[0]
	colls, err := client.QueryCollections(c.db.Self, &documentdb.Query{
		Query: "SELECT * FROM ROOT r WHERE r.id=@id",
		Parameters: []documentdb.Parameter{
			{Name: "@id", Value: creds.Collection},
		},
	})
	if err != nil {
		return err
	} else if len(colls) == 0 {
		return fmt.Errorf("collection %s for CosmosDB state store not found.  This must be created before Dapr uses it", creds.Collection)
	}

	c.collection = &colls[0]
	c.client = client

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

	b, err := jsoniter.ConfigFastest.Marshal(&items[0].Value)
	if err != nil {
		return nil, err
	}

	return &state.GetResponse{
		Data: b,
		ETag: items[0].Etag,
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

	if req.ETag != "" {
		options = append(options, documentdb.IfMatch((req.ETag)))
	}
	if req.Options.Consistency == state.Strong {
		options = append(options, documentdb.ConsistencyLevel(documentdb.Strong))
	}
	if req.Options.Consistency == state.Eventual {
		options = append(options, documentdb.ConsistencyLevel(documentdb.Eventual))
	}

	b, ok := req.Value.([]uint8)
	if ok {
		// data arrived in bytes and already json.  Don't marshal the Value field again.
		item := CosmosItemWithRawMessage{ID: req.Key, Value: b, PartitionKey: partitionKey}
		var marshalled []byte
		marshalled, err = convertToJSONWithoutEscapes(item)
		if err != nil {
			return err
		}
		_, err = c.client.UpsertDocument(c.collection.Self, marshalled, options...)
	} else {
		// data arrived as non-bytes, just pass it through.
		_, err = c.client.UpsertDocument(c.collection.Self, CosmosItem{ID: req.Key, Value: req.Value, PartitionKey: partitionKey}, options...)
	}

	if err != nil {
		if req.ETag != "" {
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

	if req.ETag != "" {
		options = append(options, documentdb.IfMatch((req.ETag)))
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

	if req.ETag != "" {
		return state.NewETagError(state.ETagMismatch, err)
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

			// Value need not be marshaled here. It is handled by cosmosdb client.
			upsertOperation := CosmosItem{
				ID:           req.Key,
				Value:        req.Value,
				PartitionKey: partitionKey,
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

// This is a helper to return the partition key to use.  If if metadata["partitionkey"] is present,
// use that, otherwise use what's in "key".
func populatePartitionMetadata(key string, requestMetadata map[string]string) string {
	if val, found := requestMetadata[metadataPartitionKey]; found {
		return val
	}

	return key
}

func convertToJSONWithoutEscapes(t interface{}) ([]byte, error) {
	buffer := &bytes.Buffer{}
	encoder := jsoniter.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(t)

	return buffer.Bytes(), err
}
