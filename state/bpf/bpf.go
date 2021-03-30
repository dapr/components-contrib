// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package bpf

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/cilium/ebpf"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/dapr/pkg/logger"
	jsoniter "github.com/json-iterator/go"
)

const (
	defaultKeySize = 4
	valueSize      = "ValueSize"
	maxEntries     = "MaxEntries"
)

// BFP map state store
type BPFMap struct {
	client *ebpf.Map
	json   jsoniter.API
	logger logger.Logger
}

// NewBPFStateStore creates a new instance of BFP map state store
func NewBPFStateStore(logger logger.Logger) *BPFMap {
	s := &BPFMap{
		json:   jsoniter.ConfigFastest,
		logger: logger,
	}
	return s
}

// Init initializes the BPF map state store
func (m *BPFMap) Init(metadata state.Metadata) error {
	err := validateMetadata(metadata)
	if err != nil {
		return err
	}
	v := metadata.Properties[valueSize]
	valueSizeValue, _ := strconv.ParseUint(v, 10, 0)
	v = metadata.Properties[maxEntries]
	maxEntriesValue, _ := strconv.ParseUint(v, 10, 0)
	newClient, err := ebpf.NewMap(&ebpf.MapSpec{
		Type:       ebpf.Hash,
		KeySize:    defaultKeySize,
		ValueSize:  uint32(valueSizeValue),
		MaxEntries: uint32(maxEntriesValue),
	})
	if err != nil {
		return err
	}
	m.client = newClient
	return nil
}

func validateMetadata(metadata state.Metadata) error {
	if metadata.Properties[valueSize] == "" {
		return errors.New("ebpf map error: valueSize is missing")
	}

	if metadata.Properties[maxEntries] == "" {
		return errors.New("ebpf map error: maxEntries is missing")
	}

	v := metadata.Properties[valueSize]
	if v != "" {
		_, err := strconv.ParseUint(v, 10, 0)
		if err != nil {
			return fmt.Errorf("ebpf map error: %v", err)
		}
	}

	v = metadata.Properties[maxEntries]
	if v != "" {
		_, err := strconv.ParseUint(v, 10, 0)
		if err != nil {
			return fmt.Errorf("ebpf map error: %v", err)
		}
	}

	return nil
}

// Features returns the features available in this state store
func (m *BPFMap) Features() []state.Feature {
	return nil
}

// Delete removes an entity from the store
func (m *BPFMap) Delete(req *state.DeleteRequest) error {
	key, _ := strconv.ParseUint(req.Key, 10, 0)
	err := m.client.Delete(uint32(key))
	if err != nil {
		return err
	}
	return nil
}

// Get returns an entity from store
func (m *BPFMap) Get(req *state.GetRequest) (*state.GetResponse, error) {
	key, _ := strconv.ParseUint(req.Key, 10, 0)
	var bt []byte = make([]byte, m.client.ValueSize())
	err := m.client.Lookup(uint32(key), bt)
	if err != nil {
		return nil, err
	}
	return &state.GetResponse{
		Data: bt,
	}, nil
}

// Set adds/updates an entity on store
func (m *BPFMap) Set(req *state.SetRequest) error {
	key, _ := strconv.ParseUint(req.Key, 10, 0)
	var bt []byte
	bt, _ = utils.Marshal(req.Value, m.json.Marshal)
	err := m.client.Put(uint32(key), bt)
	if err != nil {
		return err
	}
	return nil
}

// Close implements io.Closer
func (m *BPFMap) Close() error {
	err := m.client.Close()
	if err != nil {
		return err
	}
	return nil
}
