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

package consul

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/hashicorp/consul/api"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
	"github.com/dapr/kit/ptr"
)

// Consul is a state store implementation for HashiCorp Consul.
type Consul struct {
	state.BulkStore

	client        *api.Client
	keyPrefixPath string
	logger        logger.Logger
}

type consulConfig struct {
	Datacenter    string `json:"datacenter"`
	HTTPAddr      string `json:"httpAddr"`
	ACLToken      string `json:"aclToken"`
	Scheme        string `json:"scheme"`
	KeyPrefixPath string `json:"keyPrefixPath"`
}

// NewConsulStateStore returns a new consul state store.
func NewConsulStateStore(logger logger.Logger) state.Store {
	s := &Consul{
		logger: logger,
	}
	s.BulkStore = state.NewDefaultBulkStore(s)
	return s
}

// Init does metadata and config parsing and initializes the
// Consul client.
func (c *Consul) Init(_ context.Context, metadata state.Metadata) error {
	consulConfig, err := metadataToConfig(metadata.Properties)
	if err != nil {
		return fmt.Errorf("couldn't convert metadata properties: %s", err)
	}

	var keyPrefixPath string
	if consulConfig.KeyPrefixPath == "" {
		keyPrefixPath = "dapr"
	}

	config := &api.Config{
		Datacenter: consulConfig.Datacenter,
		Address:    consulConfig.HTTPAddr,
		Token:      consulConfig.ACLToken,
		Scheme:     consulConfig.Scheme,
	}

	client, err := api.NewClient(config)
	if err != nil {
		return fmt.Errorf("initializing consul client: %w", err)
	}

	c.client = client
	c.keyPrefixPath = keyPrefixPath

	return nil
}

// Features returns the features available in this state store.
func (c *Consul) Features() []state.Feature {
	// Etag is just returned and not handled in set or delete operations.
	return nil
}

func metadataToConfig(connInfo map[string]string) (*consulConfig, error) {
	m := &consulConfig{}
	err := kitmd.DecodeMetadata(connInfo, m)
	return m, err
}

// Get retrieves a Consul KV item.
func (c *Consul) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	queryOpts := &api.QueryOptions{}
	if req.Options.Consistency == state.Strong {
		queryOpts.RequireConsistent = true
	}
	queryOpts = queryOpts.WithContext(ctx)

	resp, queryMeta, err := c.client.KV().Get(c.keyPrefixPath+"/"+req.Key, queryOpts)
	if err != nil {
		return nil, err
	}

	if resp == nil {
		return &state.GetResponse{}, nil
	}

	return &state.GetResponse{
		Data: resp.Value,
		ETag: ptr.Of(queryMeta.LastContentHash),
	}, nil
}

// Set saves a Consul KV item.
func (c *Consul) Set(ctx context.Context, req *state.SetRequest) error {
	var reqValByte []byte
	b, ok := req.Value.([]byte)
	if ok {
		reqValByte = b
	} else {
		reqValByte, _ = json.Marshal(req.Value)
	}

	keyWithPath := c.keyPrefixPath + "/" + req.Key

	writeOptions := new(api.WriteOptions)
	writeOptions = writeOptions.WithContext(ctx)
	_, err := c.client.KV().Put(&api.KVPair{
		Key:   keyWithPath,
		Value: reqValByte,
	}, writeOptions)
	if err != nil {
		return fmt.Errorf("couldn't set key %s: %s", keyWithPath, err)
	}

	return nil
}

// Delete performes a Consul KV delete operation.
func (c *Consul) Delete(ctx context.Context, req *state.DeleteRequest) error {
	keyWithPath := c.keyPrefixPath + "/" + req.Key
	writeOptions := new(api.WriteOptions)
	writeOptions = writeOptions.WithContext(ctx)
	_, err := c.client.KV().Delete(keyWithPath, writeOptions)
	if err != nil {
		return fmt.Errorf("couldn't delete key %s: %s", keyWithPath, err)
	}

	return nil
}

func (c *Consul) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := consulConfig{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return
}

func (c *Consul) Close() error {
	return nil
}
