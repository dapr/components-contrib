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

package dcs

import (
	"encoding/json"
	"fmt"

	"github.com/dapr/components-contrib/state"
	redis "github.com/dapr/components-contrib/state/redis"
	"github.com/dapr/kit/logger"
)

type StateStore struct {
	r        *redis.StateStore
	metadata dcsMetadata
	logger   logger.Logger
}

type dcsMetadata struct {
	InstanceName string `json:"instanceName"` // (optional) name of the dcs instance
	InstanceID   string `json:"instanceId"`   // id of the dcs instance
	ProjectID    string `json:"projectId"`    // tenant id of the dcs instance
	Region       string `json:"region"`       // (optional) Huawei cloud region where the dcs instance falls into
	VPCId        string `json:"vpcId"`        // (optional) Huawei cloud vpc id of the dcs instance
	DCSHost      string `json:"dcsHost"`      // connection ip and port of the dcs instance (ip:port)
	DCSPassword  string `json:"dcsPassword"`  // password for the dcs instance connection
}

// NewStateStore returns a new DCS (redis) state store.
func NewDCSStateStore(logger logger.Logger) *StateStore {
	logger.Debugf("starting new DCS state store")
	d := &StateStore{
		logger: logger,
	}
	d.r = redis.NewRedisStateStore(logger)

	return d
}

func parseDCSMetadata(meta state.Metadata) (*dcsMetadata, error) {
	b, err := json.Marshal(meta.Properties)
	if err != nil {
		return nil, err
	}

	var m dcsMetadata
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}
	if m.InstanceID == "" {
		return nil, fmt.Errorf("missing dcs instance id")
	}
	if m.ProjectID == "" {
		return nil, fmt.Errorf("missing dcs project id")
	}
	if m.DCSHost == "" {
		return nil, fmt.Errorf("missing dcs host")
	}

	return &m, nil
}

// Init does metadata and connection parsing.
func (d *StateStore) Init(metadata state.Metadata) error {
	d.logger.Debugf("initializing DCS and parsing metadata")
	m, err := parseDCSMetadata(metadata)
	if err != nil {
		return err
	}
	d.metadata = *m

	// re-map DCS host/password to respective redis keys
	metadata.Properties["redisHost"] = metadata.Properties["dcsHost"]
	if val, ok := metadata.Properties["dcsPassword"]; ok {
		metadata.Properties["redisPassword"] = val
	} else {
		return fmt.Errorf("missing dcs password")
	}

	err = d.r.Init(metadata)
	if err != nil {
		return err
	}

	d.logger.Debugf("DCS metadata [%v]", metadata)

	return nil
}

func (d *StateStore) Ping() error {
	return d.r.Ping()
}

func (d *StateStore) BulkDelete(req []state.DeleteRequest) error {
	return d.r.BulkDelete(req)
}

func (d *StateStore) BulkGet(req []state.GetRequest) (bool, []state.BulkGetResponse, error) {
	return d.r.BulkGet(req)
}

// BulkSet performs a bulk set operation.
func (d *StateStore) BulkSet(req []state.SetRequest) error {
	return d.r.BulkSet(req)
}

// Delete performs a delete operation.
func (d *StateStore) Delete(req *state.DeleteRequest) error {
	return d.r.Delete(req)
}

// Get retrieves state from dcs with a key.
func (d *StateStore) Get(req *state.GetRequest) (*state.GetResponse, error) {
	return d.r.Get(req)
}

// Set saves into dcs.
func (d *StateStore) Set(req *state.SetRequest) error {
	return d.r.Set(req)
}

// Multi performs a transactional operation. succeeds only if all operations succeed, and fails if one or more operations fail.
func (d *StateStore) Multi(request *state.TransactionalStateRequest) error {
	return d.r.Multi(request)
}

// Features returns the features available in this state store.
func (d *StateStore) Features() []state.Feature {
	return d.r.Features()
}
