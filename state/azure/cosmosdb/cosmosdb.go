// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package cosmosdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/dapr/pkg/logger"
	jsoniter "github.com/json-iterator/go"

	"github.com/a8m/documentdb"
)

// StateStore is a CosmosDB state store
type StateStore struct {
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
	return &StateStore{logger: logger}
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
	for _, proc := range sps {
		if proc.Id == storedProcedureName {
			c.sp = &proc
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
	err := state.CheckSetRequestOptions(req)
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

	_, err = c.client.UpsertDocument(c.collection.Self, CosmosItem{ID: req.Key, Value: req.Value, PartitionKey: partitionKey}, options...)

	if err != nil {
		return err
	}

	return nil
}

// BulkSet performs a bulk set operation
func (c *StateStore) BulkSet(req []state.SetRequest) error {
	for i := range req {
		err := c.Set(&req[i])
		if err != nil {
			return err
		}
	}

	return nil
}

// Delete performs a delete operation
func (c *StateStore) Delete(req *state.DeleteRequest) error {
	err := state.CheckDeleteRequestOptions(req)
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
	return err
}

// BulkDelete performs a bulk delete operation
func (c *StateStore) BulkDelete(req []state.DeleteRequest) error {
	for i := range req {
		err := c.Delete(&req[i])
		if err != nil {
			return err
		}
	}

	return nil
}

// Multi performs a transactional operation. succeeds only if all operations succeed, and fails if one or more operations fail
func (c *StateStore) Multi(operations []state.TransactionalRequest) error {
	upserts := []CosmosItem{}
	deletes := []CosmosItem{}

	partitionKey := unknownPartitionKey
	previousPartitionKey := unknownPartitionKey

	for _, o := range operations {
		t := o.Request.(state.KeyInt)
		key := t.GetKey()
		metadata := t.GetMetadata()

		partitionKey = populatePartitionMetadata(key, metadata)
		if previousPartitionKey != unknownPartitionKey &&
			partitionKey != previousPartitionKey {
			return errors.New("all objects used in Multi() must have the same partition key")
		}
		previousPartitionKey = partitionKey

		if o.Operation == state.Upsert {
			req := o.Request.(state.SetRequest)

			upsertOperation := CosmosItem{
				ID:           req.Key,
				Value:        req.Value,
				PartitionKey: partitionKey}

			upserts = append(upserts, upsertOperation)
		} else if o.Operation == state.Delete {
			req := o.Request.(state.DeleteRequest)

			deleteOperation := CosmosItem{
				ID:           req.Key,
				Value:        "", // Value does not need to be specified
				PartitionKey: partitionKey}
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
