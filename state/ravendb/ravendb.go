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

// Package mongodb is an implementation of StateStore interface to perform operations on store

package ravendb

import (
	"context"
	"errors"
	"fmt"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	stateutils "github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
	jsoniterator "github.com/json-iterator/go"
	ravendb "github.com/ravendb/ravendb-go-client"
	"reflect"
	"time"
)

const (
	defaultDatabaseName = "daprStore"
	databaseName        = "databaseName"
	serverURL           = "serverUrl"
)

type RavenDB struct {
	state.BulkStore

	documentStore    *ravendb.DocumentStore
	operationTimeout time.Duration
	metadata         RavenDBMetadata

	features     []state.Feature
	logger       logger.Logger
	isReplicaSet bool
}

type RavenDBMetadata struct {
	DatabaseName string
	ServerURL    string
}

type Item struct {
	ID    string
	Value string
	Etag  string
	TTL   *time.Time
}

func NewRavenDB(logger logger.Logger) state.Store {
	store := &RavenDB{
		features: []state.Feature{
			state.FeatureETag,
			state.FeatureTransactional,
			state.FeatureQueryAPI,
			state.FeatureTTL,
		},
		logger: logger,
	}
	store.BulkStore = state.NewDefaultBulkStore(store)
	return store
}

func (r *RavenDB) Init(ctx context.Context, metadata state.Metadata) (err error) {
	r.metadata, err = getRavenDBMetaData(metadata)
	if err != nil {
		return err
	}

	//TODO: Operation timeout?
	store, err := r.getRavenDBStore(ctx)
	if err != nil {
		return fmt.Errorf("error in creating Raven DB Store")
	}

	r.documentStore = store

	return nil
}

// Features returns the features available in this state store.
func (r *RavenDB) Features() []state.Feature {
	return r.features
}

func (r *RavenDB) Delete(ctx context.Context, req *state.DeleteRequest) error {
	session, err := r.documentStore.OpenSession("")
	if err != nil {
		return fmt.Errorf("error opening session while deleting")
	}
	defer session.Close()

	err = r.deleteInternal(ctx, req, session)
	if err != nil {
		return fmt.Errorf("error deleting %s", req.Key)
	}

	err = session.SaveChanges()
	if err != nil {
		return fmt.Errorf("error saving changes")
	}

	return nil
}

func (r *RavenDB) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	session, err := r.documentStore.OpenSession(r.metadata.DatabaseName)
	if err != nil {
		return &state.GetResponse{}, fmt.Errorf("error opening session while storing data faild with error %s", err)
	}
	defer session.Close()

	var item *Item
	err = session.Load(&item, req.Key)
	if err != nil {
		return &state.GetResponse{}, fmt.Errorf("error storing data %s", err)
	}
	resp := &state.GetResponse{
		Data: []byte(item.Value),
	}

	return resp, nil
}

func (r *RavenDB) Set(ctx context.Context, req *state.SetRequest) error {
	session, err := r.documentStore.OpenSession(r.metadata.DatabaseName)
	if err != nil {
		return fmt.Errorf("error opening session while storing data faild with error %s", err)
	}
	defer session.Close()
	err = r.setInternal(ctx, req, session)
	if err != nil {
		return fmt.Errorf("error processing item %s", err)
	}

	err = session.SaveChanges()
	if err != nil {
		return fmt.Errorf("error saving changes %s", err)
	}
	return nil
}

func (r *RavenDB) Ping(ctx context.Context) error {
	session, err := r.documentStore.OpenSession("")
	defer session.Close()
	if err != nil {
		return fmt.Errorf("error opening session while storing data faild with error %s", err)
	}

	return nil
}

func (r *RavenDB) Multi(ctx context.Context, request *state.TransactionalStateRequest) error {
	session, err := r.documentStore.OpenSession(r.metadata.DatabaseName)
	if err != nil {
		return fmt.Errorf("error opening session while storing data faild with error %s", err)
	}
	defer session.Close()
	for _, o := range request.Operations {
		var err error
		switch req := o.(type) {
		case state.SetRequest:
			err = r.setInternal(ctx, &req, session)
		case state.DeleteRequest:
			err = r.deleteInternal(ctx, &req, session)
		}

		if err != nil {
			return fmt.Errorf("error parsing requests: %w", err)
		}
	}

	err = session.SaveChanges()
	if err != nil {
		return fmt.Errorf("error during transaction, aborting the transaction: %w", err)
	}

	return nil
}

func (r *RavenDB) marshalToString(v interface{}) (string, error) {
	if buf, ok := v.([]byte); ok {
		return string(buf), nil
	}

	return jsoniterator.ConfigFastest.MarshalToString(v)
}

func (r *RavenDB) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := RavenDBMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return
}

func (r *RavenDB) setInternal(ctx context.Context, req *state.SetRequest, session *ravendb.DocumentSession) error {
	data, err := r.marshalToString(req.Value)
	if err != nil {
		return fmt.Errorf("ravendb error: failed to marshal value for key %s: %w", req.Key, err)
	}

	item := &Item{
		ID:    req.Key,
		Value: data,
	}

	err = session.Store(item)
	if err != nil {
		return fmt.Errorf("error storing data: %s", err)
	}

	reqTTL, err := stateutils.ParseTTL(req.Metadata)
	if err != nil {
		return fmt.Errorf("failed to parse TTL: %w", err)
	}

	if reqTTL != nil {
		metaData, err := session.Advanced().GetMetadataFor(item)
		if err != nil {
			return fmt.Errorf("Failed to get metadata for item")
		}
		expiry := time.Now().Add(time.Second * time.Duration(*reqTTL)).UTC()
		iso8601String := expiry.Format("2006-01-02T15:04:05.9999999Z07:00")
		metaData.Put("@expires", iso8601String)
	}

	return nil
}

func (r *RavenDB) deleteInternal(ctx context.Context, req *state.DeleteRequest, session *ravendb.DocumentSession) error {
	err := session.DeleteByID(req.Key, "")
	if err != nil {
		return fmt.Errorf("error deleting %s", req.Key)
	}

	return nil
}

func getRavenDBMetaData(meta state.Metadata) (RavenDBMetadata, error) {
	m := RavenDBMetadata{
		DatabaseName: defaultDatabaseName,
	}

	err := kitmd.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return m, err
	}

	if m.ServerURL == "" {
		return m, errors.New("server url is required")
	}

	return m, nil
}

func (r *RavenDB) getRavenDBStore(ctx context.Context) (*ravendb.DocumentStore, error) {
	serverNodes := []string{r.metadata.ServerURL}
	store := ravendb.NewDocumentStore(serverNodes, r.metadata.DatabaseName)
	if err := store.Initialize(); err != nil {
		return nil, err
	}

	return store, nil
}
