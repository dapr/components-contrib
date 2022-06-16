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
	ctx "context"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/agrea/ptr"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const (
	keyDelimiter        = "||"
	valueEntityProperty = "Value"

	accountNameKey = "accountName"
	accountKeyKey  = "accountKey"
	tableNameKey   = "tableName"
)

type StateStore struct {
	state.DefaultBulkStore
	client *aztables.Client
	json   jsoniter.API

	features []state.Feature
	logger   logger.Logger
}

type tablesMetadata struct {
	accountName string
	accountKey  string
	tableName   string
}

// Init Initialises connection to table storage, optionally creates a table if it doesn't exist.
func (r *StateStore) Init(metadata state.Metadata) error {
	meta, err := getTablesMetadata(metadata.Properties)
	if err != nil {
		return err
	}

	cred, err := aztables.NewSharedKeyCredential(meta.accountName, meta.accountKey)
	if err != nil {
		return err
	}
	serviceURL := fmt.Sprintf("https://%s.table.core.windows.net", meta.accountName)
	client, err := aztables.NewServiceClientWithSharedKey(serviceURL, cred, nil)
	if err != nil {
		return err
	}

	_, err = client.CreateTable(ctx.Background(), meta.tableName, nil)
	if err != nil {
		if isTableAlreadyExistsError(err) {
			// error creating table, but it already exists so we're fine
			r.logger.Debugf("table already exists")
		} else {
			return err
		}
	}
	r.client = client.NewClient(meta.tableName)

	r.logger.Debugf("table initialised, account: %s, table: %s", meta.accountName, meta.tableName)

	return nil
}

// Features returns the features available in this state store.
func (r *StateStore) Features() []state.Feature {
	return r.features
}

func (r *StateStore) Delete(req *state.DeleteRequest) error {
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

func (r *StateStore) Get(req *state.GetRequest) (*state.GetResponse, error) {
	r.logger.Debugf("fetching %s", req.Key)
	pk, rk := getPartitionAndRowKey(req.Key)
	resp, err := r.client.GetEntity(ctx.Background(), pk, rk, nil)
	if err != nil {
		if isNotFoundError(err) {
			return &state.GetResponse{}, nil
		}
		return &state.GetResponse{}, err
	}

	data, etag, err := r.unmarshal(&resp)

	return &state.GetResponse{
		Data: data,
		ETag: etag,
	}, err
}

func (r *StateStore) Set(req *state.SetRequest) error {
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
	marshalledEntity, err := r.marshal(req)
	if err != nil {
		return err
	}

	// InsertOrReplace does not support ETag concurrency, therefore we will use Insert to check for key existence
	// and then use Update to update the key if it exists with the specified ETag
	_, err = r.client.AddEntity(ctx.Background(), marshalledEntity, nil)

	if err != nil {
		// If Insert failed because item already exists, try to Update instead per Upsert semantics
		if isEntityAlreadyExistsError(err) {
			// Always Update using the etag when provided even if Concurrency != FirstWrite.
			// Today the presence of etag takes precedence over Concurrency.
			// In the future #2739 will impose a breaking change which must disallow the use of etag when not using FirstWrite.
			if req.ETag != nil && *req.ETag != "" {
				etag := azcore.ETag(*req.ETag)
				_, uerr := r.client.UpdateEntity(ctx.Background(), marshalledEntity, &aztables.UpdateEntityOptions{
					IfMatch:    &etag,
					UpdateMode: aztables.UpdateModeReplace,
				})
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
				_, uerr := r.client.UpdateEntity(ctx.Background(), marshalledEntity, &aztables.UpdateEntityOptions{
					IfMatch:    nil, // this is the same as "*" matching all ETags
					UpdateMode: aztables.UpdateModeReplace,
				})
				if uerr != nil {
					return uerr
				}
			}
		} else {
			// Any other unexpected error on Insert is propagated to the caller
			return err
		}
	}

	return nil
}

func isNotFoundError(err error) bool {
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		return (respErr.ErrorCode == string(aztables.ResourceNotFound)) || (respErr.ErrorCode == string(aztables.EntityNotFound))
	}
	return false
}

func isEntityAlreadyExistsError(err error) bool {
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		return respErr.ErrorCode == string(aztables.EntityAlreadyExists)
	}
	return false
}

func isTableAlreadyExistsError(err error) bool {
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		return respErr.ErrorCode == string(aztables.TableAlreadyExists)
	}
	return false
}

func (r *StateStore) deleteRow(req *state.DeleteRequest) error {
	pk, rk := getPartitionAndRowKey(req.Key)

	if req.ETag != nil {
		azcoreETag := azcore.ETag(*req.ETag)
		_, err := r.client.DeleteEntity(ctx.Background(), pk, rk, &aztables.DeleteEntityOptions{IfMatch: &azcoreETag})
		return err
	}
	all := azcore.ETagAny
	_, err := r.client.DeleteEntity(ctx.Background(), pk, rk, &aztables.DeleteEntityOptions{IfMatch: &all})
	return err
}

func getPartitionAndRowKey(key string) (string, string) {
	pr := strings.Split(key, keyDelimiter)
	if len(pr) != 2 {
		return pr[0], ""
	}

	return pr[0], pr[1]
}

func (r *StateStore) marshal(req *state.SetRequest) ([]byte, error) {
	var value string
	b, ok := req.Value.([]byte)
	if ok {
		value = string(b)
	} else {
		value, _ = jsoniter.MarshalToString(req.Value)
	}

	pk, rk := getPartitionAndRowKey(req.Key)

	entity := aztables.EDMEntity{
		Entity: aztables.Entity{
			PartitionKey: pk,
			RowKey:       rk,
		},
		Properties: map[string]interface{}{
			valueEntityProperty: value,
		},
	}

	var etag string
	if req.ETag != nil {
		etag = *req.ETag
	}
	entity.ETag = etag

	marshalled, err := jsoniter.Marshal(entity)
	return marshalled, err
}

func (r *StateStore) unmarshal(row *aztables.GetEntityResponse) ([]byte, *string, error) {
	var myEntity aztables.EDMEntity
	err := jsoniter.Unmarshal(row.Value, &myEntity)
	if err != nil {
		return nil, nil, err
	}

	raw := myEntity.Properties[valueEntityProperty]

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
	etag := myEntity.ETag

	return []byte(sv), ptr.String(etag), nil
}
