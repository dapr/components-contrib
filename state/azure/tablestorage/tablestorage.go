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
	"context"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/agrea/ptr"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
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
	state.DefaultBulkStore
	table *storage.Table
	json  jsoniter.API

	features []state.Feature
	logger   logger.Logger
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
	userAgent := "dapr-" + logger.DaprVersion
	client.AddToUserAgent(userAgent)
	r.table = tables.GetTableReference(meta.tableName)

	// check table exists
	r.logger.Debugf("using table '%s'", meta.tableName)
	err = r.table.Create(operationTimeout, storage.FullMetadata, nil)
	if err != nil {
		if isTableAlreadyExistsError(err) {
			// error creating table, but it already exists so we're fine
			r.logger.Debugf("table already exists")
		} else {
			return err
		}
	}

	r.logger.Debugf("table initialised, account: %s, table: %s", meta.accountName, meta.tableName)

	return nil
}

// Features returns the features available in this state store.
func (r *StateStore) Features() []state.Feature {
	return r.features
}

func (r *StateStore) Delete(ctx context.Context, req *state.DeleteRequest) error {
	r.logger.Debugf("delete %s", req.Key)

	err := r.deleteRow(req)
	if err != nil {
		if req.ETag != nil {
			return state.NewETagError(state.ETagMismatch, err)
		} else if isNotFoundError(err) {
			// deleting an item that doesn't exist without specifying an ETAG is a noop
			return nil
		}
	}

	return err
}

func (r *StateStore) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
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

func (r *StateStore) Set(ctx context.Context, req *state.SetRequest) error {
	r.logger.Debugf("saving %s", req.Key)

	err := r.writeRow(req)

	return err
}

func NewAzureTablesStateStore(logger logger.Logger) *StateStore {
	s := &StateStore{
		json:     jsoniter.ConfigFastest,
		features: []state.Feature{state.FeatureETag},
		logger:   logger,
	}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
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
		return nil, errors.New(fmt.Sprintf("missing or empty %s field from metadata", accountKeyKey))
	}

	if val, ok := metadata[tableNameKey]; ok && val != "" {
		meta.tableName = val
	} else {
		return nil, errors.New(fmt.Sprintf("missing or empty %s field from metadata", tableNameKey))
	}

	return &meta, nil
}

func (r *StateStore) writeRow(req *state.SetRequest) error {
	pk, rk := getPartitionAndRowKey(req.Key)
	entity := r.table.GetEntityReference(pk, rk)
	entity.Properties = map[string]interface{}{
		valueEntityProperty: r.marshal(req),
	}

	var etag string
	if req.ETag != nil {
		etag = *req.ETag
	}
	entity.OdataEtag = etag

	// InsertOrReplace does not support ETag concurrency, therefore we will use Insert to check for key existence
	// and then use Update to update the key if it exists with the specified ETag

	err := entity.Insert(storage.FullMetadata, nil)
	if err != nil {
		// If Insert failed because item already exists, try to Update instead per Upsert semantics
		if isEntityAlreadyExistsError(err) {
			// Always Update using the etag when provided even if Concurrency != FirstWrite.
			// Today the presence of etag takes precedence over Concurrency.
			// In the future #2739 will impose a breaking change which must disallow the use of etag when not using FirstWrite.
			if etag != "" {
				uerr := entity.Update(false, nil)
				if uerr != nil {
					if isNotFoundError(uerr) {
						return state.NewETagError(state.ETagMismatch, uerr)
					}
					return uerr
				}
			} else if req.Options.Concurrency == state.FirstWrite {
				// Otherwise, if FirstWrite was set, but no etag was provided for an Update operation
				// explicitly flag it as an error.
				// entity.Update itself does not flag the test case as a mismatch as it does not distinguish
				// between nil and "" etags, the initial etag will always be "", which would match on update.
				return state.NewETagError(state.ETagMismatch, errors.New("update with Concurrency.FirstWrite without ETag"))
			} else {
				// Finally, last write semantics without ETag should always perform a force update.
				return entity.Update(true, nil)
			}
		} else {
			// Any other unexpected error on Insert is propagated to the caller
			return err
		}
	}

	return nil
}

func isNotFoundError(err error) bool {
	azureError, ok := err.(storage.AzureStorageServiceError)

	return ok && azureError.Code == "ResourceNotFound"
}

func isEntityAlreadyExistsError(err error) bool {
	azureError, ok := err.(storage.AzureStorageServiceError)

	return ok && azureError.Code == "EntityAlreadyExists"
}

func isTableAlreadyExistsError(err error) bool {
	azureError, ok := err.(storage.AzureStorageServiceError)

	return ok && azureError.Code == "TableAlreadyExists"
}

func (r *StateStore) deleteRow(req *state.DeleteRequest) error {
	pk, rk := getPartitionAndRowKey(req.Key)
	entity := r.table.GetEntityReference(pk, rk)

	if req.ETag != nil {
		entity.OdataEtag = *req.ETag

		// force=false sets the "If-Match: <ETag>" header to ensure that the delete is only performed if the
		// entity's ETag matches the specified ETag
		return entity.Delete(false, nil)
	}

	// force=true sets the "If-Match: *" header to ensure that we delete a matching entity
	// regardless of the entity's ETag value
	return entity.Delete(true, nil)
}

func (r *StateStore) Ping(ctx context.Context) error {
	return nil
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

func (r *StateStore) unmarshal(row *storage.Entity) ([]byte, *string, error) {
	raw := row.Properties[valueEntityProperty]

	// value column not present
	if raw == nil {
		return nil, nil, nil
	}

	// must be a string
	sv, ok := raw.(string)
	if !ok {
		return nil, nil, errors.New(fmt.Sprintf("expected string in column '%s'", valueEntityProperty))
	}

	// use native ETag
	etag := row.OdataEtag

	return []byte(sv), ptr.String(etag), nil
}
