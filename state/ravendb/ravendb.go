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
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
	ravendb "github.com/ravendb/ravendb-go-client"
	"time"
)

const (
	defaultDatabaseName = "daprStore"
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
	Key   string
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

func (r RavenDB) Init(ctx context.Context, metadata state.Metadata) (err error) {
	r.metadata, err = getRavenDBMetaData(metadata)
	if err != nil {
		return err
	}

	//TODO: Operation timeout?
	store, err := r.getRavenDBStore(ctx)
	if err != nil {
		return fmt.Errorf("error in creating Raven DB Store")
	}

	//TODO: Ping
	r.documentStore = store

	return nil
}

// Features returns the features available in this state store.
func (m *RavenDB) Features() []state.Feature {
	return m.features
}

func (r RavenDB) Delete(ctx context.Context, req *state.DeleteRequest) error {
	//TODO implement me
	panic("implement me")
}

func (r RavenDB) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (r RavenDB) Set(ctx context.Context, req *state.SetRequest) error {
	//TODO implement me
	panic("implement me")
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
