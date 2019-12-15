package hazelcast

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dapr/components-contrib/state"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	jsoniter "github.com/json-iterator/go"
)

const (
	hazelcastServers = "hazelcastServers"
	hazelcastMap     = "hazelcastMap"
)

//Hazelcast state store
type Hazelcast struct {
	hzMap core.Map
	json  jsoniter.API
}

// NewHazelcastStore returns a new hazelcast backed state store
func NewHazelcastStore() *Hazelcast {
	return &Hazelcast{json: jsoniter.ConfigFastest}
}

func validateMetadata(metadata state.Metadata) error {
	if metadata.Properties[hazelcastServers] == "" {
		return errors.New("hazelcast error: missing hazelcast servers")
	}
	if metadata.Properties[hazelcastMap] == "" {
		return errors.New("hazelcast error: missing hazelcast map name")
	}

	return nil
}

// Init does metadata and connection parsing
func (store *Hazelcast) Init(metadata state.Metadata) error {
	err := validateMetadata(metadata)
	if err != nil {
		return err
	}
	servers := metadata.Properties[hazelcastServers]

	hzConfig := hazelcast.NewConfig()
	hzConfig.NetworkConfig().AddAddress(strings.Split(servers, ",")...)

	client, err := hazelcast.NewClientWithConfig(hzConfig)
	if err != nil {
		return fmt.Errorf("hazelcast error: %v", err)
	}
	store.hzMap, err = client.GetMap(metadata.Properties[hazelcastMap])

	if err != nil {
		return fmt.Errorf("hazelcast error: %v", err)
	}
	return nil
}

//Set stores value for a key to Hazelcast
func (store *Hazelcast) Set(req *state.SetRequest) error {
	err := state.CheckSetRequestOptions(req)
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

// BulkSet performs a bulks save operation
func (store *Hazelcast) BulkSet(req []state.SetRequest) error {
	for _, s := range req {
		err := store.Set(&s)
		if err != nil {
			return err
		}
	}

	return nil
}

// Get retrieves state from Hazelcast with a key
func (store *Hazelcast) Get(req *state.GetRequest) (*state.GetResponse, error) {
	resp, err := store.hzMap.Get(req.Key)
	if err != nil {
		return nil, fmt.Errorf("hazelcast error: failed to get value for %s: %s", req.Key, err)
	}
	if resp == nil {
		return nil, fmt.Errorf("hazelcast error: key %s does not exist in store", req.Key)
	}
	value, err := store.json.Marshal(&resp)

	if err != nil {
		return nil, fmt.Errorf("hazelcast error: %v", err)
	}

	return &state.GetResponse{
		Data: value,
	}, nil
}

// Delete performs a delete operation
func (store *Hazelcast) Delete(req *state.DeleteRequest) error {
	err := state.CheckDeleteRequestOptions(req)
	if err != nil {
		return err
	}
	err = store.hzMap.Delete(req.Key)
	if err != nil {
		return fmt.Errorf("hazelcast error: failed to delete key - %s", req.Key)
	}
	return nil
}

// BulkDelete performs a bulk delete operation
func (store *Hazelcast) BulkDelete(req []state.DeleteRequest) error {
	for _, re := range req {
		err := store.Delete(&re)
		if err != nil {
			return err
		}
	}
	return nil
}
