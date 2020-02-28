// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

/*
Azure Table Storage state store.

Sample configuration in yaml:

	apiVersion: dapr.io/v1alpha1
	kind: Component
	metadata:
	  name: statestore
	spec:
	  type: state.azure.tablestorage
	  metadata:
	  - name: accountName
		value: <storage account name>
	  - name: accountKey
		value: <key>
	  - name: tableName
		value: <table name>

This store uses PartitionKey as service name, and RowKey as the rest of the composite key.

Concurrency is supported with ETags according to https://docs.microsoft.com/en-us/azure/storage/common/storage-concurrency#managing-concurrency-in-table-storage
*/

package tablestorage

import (
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/dapr/pkg/logger"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
)

const (
	keyDelimiter        = "||"
	valueEntityProperty = "Value"
	operationTimeout    = 1000

	accountNameKey = "accountName"
	accountKeyKey  = "accountKey"
	tableNameKey   = "tableName"
)

type StateStore struct {
	table *storage.Table
	json  jsoniter.API

	logger logger.Logger
}

type tablesMetadata struct {
	accountName string
	accountKey  string
	tableName   string
}

// Initialises connection to table storage, optionally creates a table if it doesn't exist.
func (r *StateStore) Init(metadata state.Metadata) error {
	meta, err := getTablesMetadata(metadata.Properties)
	if err != nil {
		return err
	}

	client, _ := storage.NewBasicClient(meta.accountName, meta.accountKey)
	tables := client.GetTableService()
	r.table = tables.GetTableReference(meta.tableName)

	//check table exists
	r.logger.Debugf("using table '%s'", meta.tableName)
	err = r.table.Create(operationTimeout, storage.FullMetadata, nil)
	if err != nil {
		if isTableAlreadyExistsError(err) {
			//error creating table, but it already exists so we're fine
			r.logger.Debugf("table already exists")
		} else {
			return err
		}
	}

	r.logger.Debugf("table initialised, account: %s, table: %s", meta.accountName, meta.tableName)

	return nil
}

func (r *StateStore) Delete(req *state.DeleteRequest) error {
	r.logger.Debugf("delete %s", req.Key)
	return r.deleteRow(req)
}

func (r *StateStore) BulkDelete(req []state.DeleteRequest) error {
	r.logger.Debugf("bulk delete %v key(s)", len(req))
	for _, s := range req {
		err := r.Delete(&s)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *StateStore) Get(req *state.GetRequest) (*state.GetResponse, error) {
	r.logger.Debugf("fetching %s", req.Key)
	pk, rk := getPartitionAndRowKey(req.Key)
	entity := r.table.GetEntityReference(pk, rk)
	err := entity.Get(operationTimeout, storage.FullMetadata, nil)

	if err != nil {
		if isNotFoundError(err) {
			return &state.GetResponse{}, nil
		}

		return &state.GetResponse{}, err
	}

	data, etag, err := r.unmarshal(entity)
	return &state.GetResponse{
		Data: data,
		ETag: etag,
	}, err
}

func (r *StateStore) Set(req *state.SetRequest) error {
	r.logger.Debugf("saving %s", req.Key)

	return r.writeRow(req)
}

func (r *StateStore) BulkSet(req []state.SetRequest) error {
	r.logger.Debugf("bulk set %v key(s)", len(req))

	for _, s := range req {
		err := r.Set(&s)
		if err != nil {
			return err
		}
	}

	return nil
}

func NewAzureTablesStateStore(logger logger.Logger) *StateStore {
	return &StateStore{
		json:   jsoniter.ConfigFastest,
		logger: logger,
	}
}

func getTablesMetadata(metadata map[string]string) (*tablesMetadata, error) {
	meta := tablesMetadata{}

	if val, ok := metadata[accountNameKey]; ok && val != "" {
		meta.accountName = val
	} else {
		return nil, errors.New(fmt.Sprintf("missing or empty %s field from metadata", accountNameKey))
	}

	if val, ok := metadata[accountKeyKey]; ok && val != "" {
		meta.accountKey = val
	} else {
		return nil, errors.New(fmt.Sprintf("missing of empty %s field from metadata", accountKeyKey))
	}

	if val, ok := metadata[tableNameKey]; ok && val != "" {
		meta.tableName = val
	} else {
		return nil, errors.New(fmt.Sprintf("missing of empty %s field from metadata", tableNameKey))
	}

	return &meta, nil
}

func (r *StateStore) writeRow(req *state.SetRequest) error {
	pk, rk := getPartitionAndRowKey(req.Key)
	entity := r.table.GetEntityReference(pk, rk)
	entity.Properties = map[string]interface{}{
		valueEntityProperty: r.marshal(req),
	}
	entity.OdataEtag = req.ETag

	// InsertOrReplace does not support ETag concurrency, therefore we will try to use Update method first
	// as it's more frequent, and then Insert

	err := entity.Update(false, nil)
	if err != nil {
		if isNotFoundError(err) {
			// When entity is not found (set state first time) create it
			entity.OdataEtag = ""
			return entity.Insert(storage.FullMetadata, nil)
		}
	}
	return err
}

func isNotFoundError(err error) bool {
	azureError, ok := err.(storage.AzureStorageServiceError)
	return ok && azureError.Code == "ResourceNotFound"
}

func isTableAlreadyExistsError(err error) bool {
	azureError, ok := err.(storage.AzureStorageServiceError)
	return ok && azureError.Code == "TableAlreadyExists"
}

func (r *StateStore) deleteRow(req *state.DeleteRequest) error {
	pk, rk := getPartitionAndRowKey(req.Key)
	entity := r.table.GetEntityReference(pk, rk)
	entity.OdataEtag = req.ETag
	return entity.Delete(true, nil)
}

func getPartitionAndRowKey(key string) (string, string) {
	pr := strings.Split(key, keyDelimiter)
	if len(pr) != 2 {
		return pr[0], ""
	}
	return pr[0], pr[1]
}

func (r *StateStore) marshal(req *state.SetRequest) string {
	var v string
	b, ok := req.Value.([]byte)
	if ok {
		v = string(b)
	} else {
		v, _ = jsoniter.MarshalToString(req.Value)
	}
	return v
}

func (r *StateStore) unmarshal(row *storage.Entity) ([]byte, string, error) {
	raw := row.Properties[valueEntityProperty]

	// value column not present
	if raw == nil {
		return nil, "", nil
	}

	// must be a string
	sv, ok := raw.(string)
	if !ok {
		return nil, "", errors.New(fmt.Sprintf("expected string in column '%s'", valueEntityProperty))
	}

	// use native ETag
	etag := row.OdataEtag
	return []byte(sv), etag, nil
}
