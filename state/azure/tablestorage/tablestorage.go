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
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"

	azauth "github.com/dapr/components-contrib/authentication/azure"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const (
	keyDelimiter        = "||"
	valueEntityProperty = "Value"

	accountNameKey     = "accountName"
	accountKeyKey      = "accountKey"
	tableNameKey       = "tableName"
	cosmosDbModeKey    = "cosmosDbMode"
	serviceURLKey      = "serviceURL"
	skipCreateTableKey = "skipCreateTable"
	timeout            = 15 * time.Second
)

type StateStore struct {
	state.DefaultBulkStore
	client       *aztables.Client
	json         jsoniter.API
	cosmosDbMode bool

	features []state.Feature
	logger   logger.Logger
}

type tablesMetadata struct {
	accountName     string
	accountKey      string // optional, if not provided, will use Azure AD authentication
	tableName       string
	cosmosDbMode    bool   // if true, use CosmosDB Table API, otherwise use Azure Table Storage
	serviceURL      string // optional, if not provided, will use default Azure service URL
	skipCreateTable bool   // skip attempt to create table - useful for fine grained AAD roles
}

// Init Initialises connection to table storage, optionally creates a table if it doesn't exist.
func (r *StateStore) Init(metadata state.Metadata) error {
	meta, err := getTablesMetadata(metadata.Properties)
	if err != nil {
		return err
	}

	var client *aztables.ServiceClient

	r.cosmosDbMode = meta.cosmosDbMode
	serviceURL := meta.serviceURL

	if serviceURL == "" {
		if r.cosmosDbMode {
			serviceURL = fmt.Sprintf("https://%s.table.cosmos.azure.com", meta.accountName)
		} else {
			serviceURL = fmt.Sprintf("https://%s.table.core.windows.net", meta.accountName)
		}
	}

	if meta.accountKey != "" {
		// use shared key authentication
		cred, innerErr := aztables.NewSharedKeyCredential(meta.accountName, meta.accountKey)
		if innerErr != nil {
			return innerErr
		}

		client, innerErr = aztables.NewServiceClientWithSharedKey(serviceURL, cred, nil)
		if innerErr != nil {
			return innerErr
		}
	} else {
		// fallback to azure AD authentication
		var settings azauth.EnvironmentSettings
		var innerErr error
		if r.cosmosDbMode {
			settings, innerErr = azauth.NewEnvironmentSettings("cosmosdb", metadata.Properties)
		} else {
			settings, innerErr = azauth.NewEnvironmentSettings("storage", metadata.Properties)
		}
		if innerErr != nil {
			return innerErr
		}

		token, innerErr := settings.GetTokenCredential()
		if innerErr != nil {
			return innerErr
		}
		client, innerErr = aztables.NewServiceClient(serviceURL, token, nil)
		if err != nil {
			return innerErr
		}
	}

	if !meta.skipCreateTable {
		createContext, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		_, innerErr := client.CreateTable(createContext, meta.tableName, nil)
		if innerErr != nil {
			if isTableAlreadyExistsError(innerErr) {
				// error creating table, but it already exists so we're fine
				r.logger.Debugf("table already exists")
			} else {
				return innerErr
			}
		}
	}
	r.client = client.NewClient(meta.tableName)

	r.logger.Debugf("table initialised, account: %s, cosmosDbMode: %s, table: %s", meta.accountName, meta.cosmosDbMode, meta.tableName)

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
	pk, rk := getPartitionAndRowKey(req.Key, r.cosmosDbMode)
	getContext, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	resp, err := r.client.GetEntity(getContext, pk, rk, nil)
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
		meta.accountKey = ""
	}

	if val, ok := metadata[tableNameKey]; ok && val != "" {
		meta.tableName = val
	} else {
		return nil, errors.New(fmt.Sprintf("missing or empty %s field from metadata", tableNameKey))
	}

	if val, ok := metadata[cosmosDbModeKey]; ok && val != "" {
		switch strings.ToLower(strings.TrimSpace(val)) {
		case "y", "yes", "true", "t", "on", "1":
			meta.cosmosDbMode = true
		default:
			meta.cosmosDbMode = false
		}
	} else {
		meta.cosmosDbMode = false
	}

	if val, ok := metadata[serviceURLKey]; ok && val != "" {
		meta.serviceURL = val
	} else {
		meta.serviceURL = ""
	}

	if val, ok := metadata[skipCreateTableKey]; ok && val != "" {
		switch strings.ToLower(strings.TrimSpace(val)) {
		case "y", "yes", "true", "t", "on", "1":
			meta.skipCreateTable = true
		default:
			meta.skipCreateTable = false
		}
	} else {
		meta.skipCreateTable = false
	}

	return &meta, nil
}

func (r *StateStore) writeRow(req *state.SetRequest) error {
	marshalledEntity, err := r.marshal(req)
	if err != nil {
		return err
	}

	writeContext, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// InsertOrReplace does not support ETag concurrency, therefore we will use Insert to check for key existence
	// and then use Update to update the key if it exists with the specified ETag
	_, err = r.client.AddEntity(writeContext, marshalledEntity, nil)

	if err != nil {
		// If Insert failed because item already exists, try to Update instead per Upsert semantics
		if isEntityAlreadyExistsError(err) {
			updateContext, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			// Always Update using the etag when provided even if Concurrency != FirstWrite.
			// Today the presence of etag takes precedence over Concurrency.
			// In the future #2739 will impose a breaking change which must disallow the use of etag when not using FirstWrite.
			if req.ETag != nil && *req.ETag != "" {
				etag := azcore.ETag(*req.ETag)

				_, uerr := r.client.UpdateEntity(updateContext, marshalledEntity, &aztables.UpdateEntityOptions{
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
				_, uerr := r.client.UpdateEntity(updateContext, marshalledEntity, &aztables.UpdateEntityOptions{
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
	pk, rk := getPartitionAndRowKey(req.Key, r.cosmosDbMode)

	deleteContext, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if req.ETag != nil {
		azcoreETag := azcore.ETag(*req.ETag)
		_, err := r.client.DeleteEntity(deleteContext, pk, rk, &aztables.DeleteEntityOptions{IfMatch: &azcoreETag})
		return err
	}
	all := azcore.ETagAny
	_, err := r.client.DeleteEntity(deleteContext, pk, rk, &aztables.DeleteEntityOptions{IfMatch: &all})
	return err
}

func getPartitionAndRowKey(key string, cosmosDBmode bool) (string, string) {
	pr := strings.Split(key, keyDelimiter)
	if len(pr) != 2 {
		if cosmosDBmode {
			return pr[0], "_dapr_empty_row_key_value_"
		} else {
			return pr[0], ""
		}
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

	pk, rk := getPartitionAndRowKey(req.Key, r.cosmosDbMode)

	entity := aztables.EDMEntity{
		Entity: aztables.Entity{
			PartitionKey: pk,
			RowKey:       rk,
		},
		Properties: map[string]interface{}{
			valueEntityProperty: value,
		},
	}

	if req.ETag != nil {
		entity.ETag = *req.ETag
	}

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
	return []byte(sv), &myEntity.ETag, nil
}
