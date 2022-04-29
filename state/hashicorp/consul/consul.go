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

	"github.com/agrea/ptr"
	"github.com/hashicorp/consul/api"
	"github.com/pkg/errors"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

// Consul is a state store implementation for HashiCorp Consul.
type Consul struct {
	state.DefaultBulkStore
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
func NewConsulStateStore(logger logger.Logger) *Consul {
	s := &Consul{logger: logger}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
}

// Init does metadata and config parsing and initializes the
// Consul client.
func (c *Consul) Init(metadata state.Metadata) error {
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
		return errors.Wrap(err, "initializing consul client")
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
	b, err := json.Marshal(connInfo)
	if err != nil {
		return nil, err
	}

	var config consulConfig
	err = json.Unmarshal(b, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

// Get retrieves a Consul KV item.
func (c *Consul) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	queryOpts := &api.QueryOptions{}
	if req.Options.Consistency == state.Strong {
		queryOpts.RequireConsistent = true
	}

	resp, queryMeta, err := c.client.KV().Get(fmt.Sprintf("%s/%s", c.keyPrefixPath, req.Key), queryOpts)
	if err != nil {
		return nil, err
	}

	if resp == nil {
		return &state.GetResponse{}, nil
	}

	return &state.GetResponse{
		Data: resp.Value,
		ETag: ptr.String(queryMeta.LastContentHash),
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

	keyWithPath := fmt.Sprintf("%s/%s", c.keyPrefixPath, req.Key)

	_, err := c.client.KV().Put(&api.KVPair{
		Key:   keyWithPath,
		Value: reqValByte,
	}, nil)
	if err != nil {
		return fmt.Errorf("couldn't set key %s: %s", keyWithPath, err)
	}

	return nil
}

func (c *Consul) Ping(ctx context.Context) error {
	return nil
}

// Delete performes a Consul KV delete operation.
func (c *Consul) Delete(ctx context.Context, req *state.DeleteRequest) error {
	keyWithPath := fmt.Sprintf("%s/%s", c.keyPrefixPath, req.Key)
	_, err := c.client.KV().Delete(keyWithPath, nil)
	if err != nil {
		return fmt.Errorf("couldn't delete key %s: %s", keyWithPath, err)
	}

	return nil
}
