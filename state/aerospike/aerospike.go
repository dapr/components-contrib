// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package aerospike

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/types"
	"github.com/agrea/ptr"
	jsoniter "github.com/json-iterator/go"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

// metadata values
const (
	hosts     = "hosts"
	namespace = "namespace"
	set       = "set" // optional
)

var (
	errMissingHosts = errors.New("aerospike: value for 'hosts' missing")
	errInvalidHosts = errors.New("aerospike: invalid value for hosts")
)

// Aerospike is a state store
type Aerospike struct {
	state.DefaultBulkStore
	namespace string
	set       string // optional
	client    *as.Client
	json      jsoniter.API

	features []state.Feature
	logger   logger.Logger
}

// NewAerospikeStateStore returns a new Aerospike state store
func NewAerospikeStateStore(logger logger.Logger) state.Store {
	s := &Aerospike{
		json:     jsoniter.ConfigFastest,
		features: []state.Feature{state.FeatureETag},
		logger:   logger,
	}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
}

func validateMetadata(metadata state.Metadata) error {
	if metadata.Properties[hosts] == "" {
		return errMissingHosts
	}
	if metadata.Properties[namespace] == "" {
		return errMissingHosts
	}

	// format is host1:port1,host2:port2
	hostsMeta := metadata.Properties[hosts]
	_, err := parseHosts(hostsMeta)
	if err != nil {
		return err
	}

	return nil
}

// Init does metadata and connection parsing
func (aspike *Aerospike) Init(metadata state.Metadata) error {
	err := validateMetadata(metadata)
	if err != nil {
		return err
	}

	hostsMeta := metadata.Properties[hosts]
	hostPorts, _ := parseHosts(hostsMeta)

	c, err := as.NewClientWithPolicyAndHost(nil, hostPorts...)
	if err != nil {
		return fmt.Errorf("aerospike: failed to connect %v", err)
	}
	aspike.client = c
	aspike.namespace = metadata.Properties[namespace]
	aspike.set = metadata.Properties[set]

	return nil
}

// Features returns the features available in this state store
func (aspike *Aerospike) Features() []state.Feature {
	return aspike.features
}

// Set stores value for a key to Aerospike. It honors ETag (for concurrency) and consistency settings
func (aspike *Aerospike) Set(req *state.SetRequest) error {
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
	if req.ETag != nil {
		var gen uint32
		gen, err = convertETag(*req.ETag)
		if err != nil {
			return err
		}
		// pass etag and fail writes is etag in DB is not same as passed by dapr (EXPECT_GEN_EQUAL)
		writePolicy.Generation = gen
		writePolicy.GenerationPolicy = as.EXPECT_GEN_EQUAL
	}

	if req.Options.Consistency == state.Strong {
		// COMMIT_ALL indicates the server should wait until successfully committing master and all replicas.
		writePolicy.CommitLevel = as.COMMIT_ALL
	} else {
		writePolicy.CommitLevel = as.COMMIT_MASTER
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
		if req.ETag != nil {
			return state.NewETagError(state.ETagMismatch, err)
		}

		return fmt.Errorf("aerospike: failed to save value for key %s - %v", req.Key, err)
	}

	return nil
}

// Get retrieves state from Aerospike with a key
func (aspike *Aerospike) Get(req *state.GetRequest) (*state.GetResponse, error) {
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
		if err == types.ErrKeyNotFound {
			return &state.GetResponse{}, nil
		}

		return nil, fmt.Errorf("aerospike: failed to get value for key %s - %v", req.Key, err)
	}
	value, err := aspike.json.Marshal(record.Bins)
	if err != nil {
		return nil, err
	}

	return &state.GetResponse{
		Data: value,
		ETag: ptr.String(strconv.FormatUint(uint64(record.Generation), 10)),
	}, nil
}

// Delete performs a delete operation
func (aspike *Aerospike) Delete(req *state.DeleteRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}
	writePolicy := &as.WritePolicy{}

	if req.ETag != nil {
		var gen uint32
		gen, err = convertETag(*req.ETag)
		if err != nil {
			return err
		}
		// pass etag and fail writes is etag in DB is not same as passed by dapr (EXPECT_GEN_EQUAL)
		writePolicy.Generation = gen
		writePolicy.GenerationPolicy = as.EXPECT_GEN_EQUAL
	}

	if req.Options.Consistency == state.Strong {
		// COMMIT_ALL indicates the server should wait until successfully committing master and all replicas.
		writePolicy.CommitLevel = as.COMMIT_ALL
	} else {
		writePolicy.CommitLevel = as.COMMIT_MASTER
	}

	asKey, err := as.NewKey(aspike.namespace, aspike.set, req.Key)
	if err != nil {
		return err
	}

	_, err = aspike.client.Delete(writePolicy, asKey)
	if err != nil {
		if req.ETag != nil {
			return state.NewETagError(state.ETagMismatch, err)
		}

		return fmt.Errorf("aerospike: failed to delete key %s - %v", req.Key, err)
	}

	return nil
}

func (aspike *Aerospike) Ping() error {
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
