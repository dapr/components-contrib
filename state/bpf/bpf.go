// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

// +build linux
// +build amd64

package bpf

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/cilium/ebpf"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	jsoniter "github.com/json-iterator/go"
)

const (
	fileName = "FileName"
)

type Map struct {
	state.DefaultBulkStore
	client *ebpf.Map
	json   jsoniter.API
	logger logger.Logger
}

// NewBPFStateStore creates a new instance of BFP map state store
func NewBPFStateStore(logger logger.Logger) *Map {
	s := &Map{
		json:   jsoniter.ConfigFastest,
		logger: logger,
	}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
}

// Init initializes the BPF map state store
func (m *Map) Init(metadata state.Metadata) error {
	if metadata.Properties[fileName] == "" {
		return errors.New("ebpf map error: FileName is missing")
	}
	newClient, err := ebpf.LoadPinnedMap(metadata.Properties[fileName], &ebpf.LoadPinOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	m.client = newClient

	return nil
}

// Features returns the features available in this state store
func (m *Map) Features() []state.Feature {
	return nil
}

// Delete removes an entity from the store
func (m *Map) Delete(req *state.DeleteRequest) error {
	return fmt.Errorf("delete operation not supported")
}

// Get returns an entity from store
func (m *Map) Get(req *state.GetRequest) (*state.GetResponse, error) {
	key, _ := strconv.ParseUint(req.Key, 10, 0)
	var bt = make([]byte, m.client.ValueSize())
	err := m.client.Lookup(uint32(key), bt)
	if err != nil {
		return nil, err
	}

	return &state.GetResponse{
		Data: bt,
	}, nil
}

// Set adds/updates an entity on store
func (m *Map) Set(req *state.SetRequest) error {
	return fmt.Errorf("set operation not supported")
}

// Close implements io.Closer
func (m *Map) Close() error {
	return m.client.Close()
}
