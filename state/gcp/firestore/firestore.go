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

package firestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"cloud.google.com/go/datastore"
	jsoniter "github.com/json-iterator/go"
	"google.golang.org/api/option"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const defaultEntityKind = "DaprState"

// Firestore State Store.
type Firestore struct {
	state.DefaultBulkStore
	client     *datastore.Client
	entityKind string

	logger logger.Logger
}

type firestoreMetadata struct {
	Type                string `json:"type" mapstructure:"type"`
	ProjectID           string `json:"project_id" mapstructure:"project_id"`
	PrivateKeyID        string `json:"private_key_id" mapstructure:"private_key_id"`
	PrivateKey          string `json:"private_key" mapstructure:"private_key"`
	ClientEmail         string `json:"client_email" mapstructure:"client_email"`
	ClientID            string `json:"client_id" mapstructure:"client_id"`
	AuthURI             string `json:"auth_uri" mapstructure:"auth_uri"`
	TokenURI            string `json:"token_uri" mapstructure:"token_uri"`
	AuthProviderCertURL string `json:"auth_provider_x509_cert_url" mapstructure:"auth_provider_x509_cert_url"`
	ClientCertURL       string `json:"client_x509_cert_url" mapstructure:"client_x509_cert_url"`
	EntityKind          string `json:"entity_kind" mapstructure:"entity_kind"`
}

type StateEntity struct {
	Value string
}

func NewFirestoreStateStore(logger logger.Logger) state.Store {
	s := &Firestore{logger: logger}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
}

// Init does metadata and connection parsing.
func (f *Firestore) Init(metadata state.Metadata) error {
	meta, err := getFirestoreMetadata(metadata)
	if err != nil {
		return err
	}
	b, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	opt := option.WithCredentialsJSON(b)
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, meta.ProjectID, opt)
	if err != nil {
		return err
	}

	f.client = client
	f.entityKind = meta.EntityKind

	return nil
}

// Features returns the features available in this state store.
func (f *Firestore) Features() []state.Feature {
	return nil
}

// Get retrieves state from Firestore with a key (Always strong consistency).
func (f *Firestore) Get(req *state.GetRequest) (*state.GetResponse, error) {
	key := req.Key

	entityKey := datastore.NameKey(f.entityKind, key, nil)
	var entity StateEntity
	err := f.client.Get(context.Background(), entityKey, &entity)

	if err != nil && !errors.Is(err, datastore.ErrNoSuchEntity) {
		return nil, err
	} else if errors.Is(err, datastore.ErrNoSuchEntity) {
		return &state.GetResponse{}, nil
	}

	return &state.GetResponse{
		Data: []byte(entity.Value),
	}, nil
}

// Set saves state into Firestore.
func (f *Firestore) Set(req *state.SetRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	var v string
	b, ok := req.Value.([]byte)
	if ok {
		v = string(b)
	} else {
		v, _ = jsoniter.MarshalToString(req.Value)
	}

	entity := &StateEntity{
		Value: v,
	}
	ctx := context.Background()
	key := datastore.NameKey(f.entityKind, req.Key, nil)

	_, err = f.client.Put(ctx, key, entity)

	if err != nil {
		return err
	}

	return nil
}

// Delete performs a delete operation.
func (f *Firestore) Delete(req *state.DeleteRequest) error {
	ctx := context.Background()
	key := datastore.NameKey(f.entityKind, req.Key, nil)

	err := f.client.Delete(ctx, key)
	if err != nil {
		return err
	}

	return nil
}

func getFirestoreMetadata(meta state.Metadata) (*firestoreMetadata, error) {
	m := firestoreMetadata{
		EntityKind: defaultEntityKind,
	}

	err := metadata.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return nil, err
	}

	requiredMetaProperties := []string{
		"type", "project_id", "private_key_id", "private_key", "client_email", "client_id",
		"auth_uri", "token_uri", "auth_provider_x509_cert_url", "client_x509_cert_url",
	}

	metadataMap := map[string]string{}
	bytes, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(bytes, &metadataMap)
	if err != nil {
		return nil, err
	}

	for _, k := range requiredMetaProperties {
		if val, ok := metadataMap[k]; !ok || len(val) < 1 {
			return nil, fmt.Errorf("error parsing required field: %s", k)
		}
	}

	return &m, nil
}

func (f *Firestore) GetComponentMetadata() map[string]string {
	metadataStruct := firestoreMetadata{}
	metadataInfo := map[string]string{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo)
	return metadataInfo
}
