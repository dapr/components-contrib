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

package aerospike

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	as "github.com/aerospike/aerospike-client-go/v6"
	"github.com/aerospike/aerospike-client-go/v6/types"
	jsoniter "github.com/json-iterator/go"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
	"github.com/dapr/kit/ptr"
)

type aerospikeMetadata struct {
	Hosts     string
	Namespace string
	Set       string // optional
}

var (
	errMissingHosts = errors.New("aerospike: value for 'hosts' missing")
	errInvalidHosts = errors.New("aerospike: invalid value for hosts")
)

// Aerospike is a state store.
type Aerospike struct {
	state.BulkStore

	namespace string
	set       string // optional
	client    *as.Client
	json      jsoniter.API

	features []state.Feature
	logger   logger.Logger
}

// NewAerospikeStateStore returns a new Aerospike state store.
func NewAerospikeStateStore(logger logger.Logger) state.Store {
	s := &Aerospike{
		json:     jsoniter.ConfigFastest,
		features: []state.Feature{state.FeatureETag},
		logger:   logger,
	}
	s.BulkStore = state.NewDefaultBulkStore(s)
	return s
}

func parseAndValidateMetadata(meta state.Metadata) (*aerospikeMetadata, error) {
	var m aerospikeMetadata
	decodeErr := kitmd.DecodeMetadata(meta.Properties, &m)
	if decodeErr != nil {
		return nil, decodeErr
	}

	if m.Hosts == "" {
		return nil, errMissingHosts
	}
	if m.Namespace == "" {
		return nil, errMissingHosts
	}

	// format is host1:port1,host2:port2
	_, err := parseHosts(m.Hosts)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

// Init does metadata and connection parsing.
func (aspike *Aerospike) Init(_ context.Context, metadata state.Metadata) error {
	m, err := parseAndValidateMetadata(metadata)
	if err != nil {
		return err
	}

	hostsMeta := m.Hosts
	hostPorts, _ := parseHosts(hostsMeta)

	c, err := as.NewClientWithPolicyAndHost(nil, hostPorts...)
	if err != nil {
		return fmt.Errorf("aerospike: failed to connect %v", err)
	}
	aspike.client = c
	aspike.namespace = m.Namespace
	aspike.set = m.Set

	return nil
}

// Features returns the features available in this state store.
func (aspike *Aerospike) Features() []state.Feature {
	return aspike.features
}

// Set stores value for a key to Aerospike. It honors ETag (for concurrency) and consistency settings.
func (aspike *Aerospike) Set(ctx context.Context, req *state.SetRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}
	asKey, err := as.NewKey(aspike.namespace, aspike.set, req.Key)
	if err != nil {
		return err
	}
	writePolicy := &as.WritePolicy{}

	// not a new record
	if req.HasETag() {
		var gen uint32
		gen, err = convertETag(*req.ETag)
		if err != nil {
			return err
		}
		// pass etag and fail writes is etag in DB is not same as passed by dapr (EXPECT_GEN_EQUAL)
		writePolicy.Generation = gen
		writePolicy.GenerationPolicy = as.EXPECT_GEN_EQUAL //nolint:nosnakecase
	}

	if req.Options.Consistency == state.Strong {
		// COMMIT_ALL indicates the server should wait until successfully committing master and all replicas.
		writePolicy.CommitLevel = as.COMMIT_ALL //nolint:nosnakecase
	} else {
		writePolicy.CommitLevel = as.COMMIT_MASTER //nolint:nosnakecase
	}

	data := make(map[string]interface{})
	arr, err := json.Marshal(req.Value)
	if err != nil {
		return err
	}
	err = json.Unmarshal(arr, &data)
	if err != nil {
		return err
	}
	err = aspike.client.Put(writePolicy, asKey, as.BinMap(data))
	if err != nil {
		if req.HasETag() {
			return state.NewETagError(state.ETagMismatch, err)
		}

		return fmt.Errorf("aerospike: failed to save value for key %s - %v", req.Key, err)
	}

	return nil
}

// Get retrieves state from Aerospike with a key.
func (aspike *Aerospike) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	asKey, err := as.NewKey(aspike.namespace, aspike.set, req.Key)
	if err != nil {
		return nil, err
	}

	policy := &as.BasePolicy{}
	if req.Options.Consistency == state.Strong {
		policy.ReadModeAP = as.ReadModeAPAll
		policy.ReadModeSC = as.ReadModeSCLinearize
	} else {
		policy.ReadModeAP = as.ReadModeAPOne
		policy.ReadModeSC = as.ReadModeSCSession
	}
	record, err := aspike.client.Get(policy, asKey)
	if err != nil {
		if err.Matches(types.KEY_NOT_FOUND_ERROR) {
			return &state.GetResponse{}, nil
		}

		return nil, fmt.Errorf("aerospike: failed to get value for key %s - %v", req.Key, err)
	}
	value, jsonErr := aspike.json.Marshal(record.Bins)
	if err != nil {
		return nil, jsonErr
	}

	return &state.GetResponse{
		Data: value,
		ETag: ptr.Of(strconv.FormatUint(uint64(record.Generation), 10)),
	}, nil
}

// Delete performs a delete operation.
func (aspike *Aerospike) Delete(ctx context.Context, req *state.DeleteRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}
	writePolicy := &as.WritePolicy{}

	if req.HasETag() {
		var gen uint32
		gen, err = convertETag(*req.ETag)
		if err != nil {
			return err
		}
		// pass etag and fail writes is etag in DB is not same as passed by dapr (EXPECT_GEN_EQUAL)
		writePolicy.Generation = gen
		writePolicy.GenerationPolicy = as.EXPECT_GEN_EQUAL //nolint:nosnakecase
	}

	if req.Options.Consistency == state.Strong {
		// COMMIT_ALL indicates the server should wait until successfully committing master and all replicas.
		writePolicy.CommitLevel = as.COMMIT_ALL //nolint:nosnakecase
	} else {
		writePolicy.CommitLevel = as.COMMIT_MASTER //nolint:nosnakecase
	}

	asKey, err := as.NewKey(aspike.namespace, aspike.set, req.Key)
	if err != nil {
		return err
	}

	_, err = aspike.client.Delete(writePolicy, asKey)
	if err != nil {
		if req.HasETag() {
			return state.NewETagError(state.ETagMismatch, err)
		}

		return fmt.Errorf("aerospike: failed to delete key %s - %v", req.Key, err)
	}

	return nil
}

func (aspike *Aerospike) Close() error {
	return nil
}

func parseHosts(hostsMeta string) ([]*as.Host, error) {
	hostPorts := []*as.Host{}
	for _, hostPort := range strings.Split(hostsMeta, ",") {
		if !strings.Contains(hostPort, ":") {
			return nil, errInvalidHosts
		}
		host := strings.Split(hostPort, ":")[0]
		port, err := strconv.ParseUint(strings.Split(hostPort, ":")[1], 10, 32)
		if err != nil {
			return nil, errInvalidHosts
		}
		//nolint:gosec
		hostPorts = append(hostPorts, as.NewHost(host, int(port)))
	}

	return hostPorts, nil
}

func convertETag(eTag string) (uint32, error) {
	i, err := strconv.ParseUint(eTag, 10, 32)
	if err != nil {
		return 0, state.NewETagError(state.ETagInvalid, err)
	}

	return uint32(i), nil
}

func (aspike *Aerospike) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := aerospikeMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return
}
