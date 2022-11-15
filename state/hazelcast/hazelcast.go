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

package hazelcast

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	jsoniter "github.com/json-iterator/go"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

// Hazelcast state store.
type Hazelcast struct {
	state.DefaultBulkStore
	hzMap  core.Map
	json   jsoniter.API
	logger logger.Logger
}

type hazelcastMetadata struct {
	HazelcastServers string
	HazelcastMap     string
}

// NewHazelcastStore returns a new hazelcast backed state store.
func NewHazelcastStore(logger logger.Logger) state.Store {
	s := &Hazelcast{
		json:   jsoniter.ConfigFastest,
		logger: logger,
	}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
}

func validateAndParseMetadata(meta state.Metadata) (*hazelcastMetadata, error) {
	m := &hazelcastMetadata{}
	err := metadata.DecodeMetadata(meta.Properties, m)
	if err != nil {
		return nil, err
	}
	if m.HazelcastServers == "" {
		return nil, fmt.Errorf("hazelcast error: missing hazelcast servers")
	}
	if m.HazelcastMap == "" {
		return nil, fmt.Errorf("hazelcast error: missing hazelcast map name")
	}

	return m, nil
}

// Init does metadata and connection parsing.
func (store *Hazelcast) Init(metadata state.Metadata) error {
	meta, err := validateAndParseMetadata(metadata)
	if err != nil {
		return err
	}
	servers := meta.HazelcastServers

	hzConfig := hazelcast.NewConfig()
	hzConfig.NetworkConfig().AddAddress(strings.Split(servers, ",")...)

	client, err := hazelcast.NewClientWithConfig(hzConfig)
	if err != nil {
		return fmt.Errorf("hazelcast error: %v", err)
	}
	store.hzMap, err = client.GetMap(meta.HazelcastMap)

	if err != nil {
		return fmt.Errorf("hazelcast error: %v", err)
	}

	return nil
}

// Features returns the features available in this state store.
func (store *Hazelcast) Features() []state.Feature {
	return nil
}

// Set stores value for a key to Hazelcast.
func (store *Hazelcast) Set(req *state.SetRequest) error {
	err := state.CheckRequestOptions(req)
	if err != nil {
		return err
	}

	var value string
	b, ok := req.Value.([]byte)
	if ok {
		value = string(b)
	} else {
		value, err = store.json.MarshalToString(req.Value)
		if err != nil {
			return fmt.Errorf("hazelcast error: failed to set key %s: %s", req.Key, err)
		}
	}
	_, err = store.hzMap.Put(req.Key, value)

	if err != nil {
		return fmt.Errorf("hazelcast error: failed to set key %s: %s", req.Key, err)
	}

	return nil
}

// Get retrieves state from Hazelcast with a key.
func (store *Hazelcast) Get(req *state.GetRequest) (*state.GetResponse, error) {
	resp, err := store.hzMap.Get(req.Key)
	if err != nil {
		return nil, fmt.Errorf("hazelcast error: failed to get value for %s: %s", req.Key, err)
	}

	// HZ Get API returns nil response if key does not exist in the map
	if resp == nil {
		return &state.GetResponse{}, nil
	}
	value, err := store.json.Marshal(&resp)
	if err != nil {
		return nil, fmt.Errorf("hazelcast error: %v", err)
	}

	return &state.GetResponse{
		Data: value,
	}, nil
}

// Delete performs a delete operation.
func (store *Hazelcast) Delete(req *state.DeleteRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}
	err = store.hzMap.Delete(req.Key)
	if err != nil {
		return fmt.Errorf("hazelcast error: failed to delete key - %s", req.Key)
	}

	return nil
}

func (store *Hazelcast) GetComponentMetadata() map[string]string {
	metadataStruct := hazelcastMetadata{}
	metadataInfo := map[string]string{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo)
	return metadataInfo
}
