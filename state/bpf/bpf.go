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
	"github.com/dapr/dapr/pkg/logger"
	jsoniter "github.com/json-iterator/go"
)

const (
	fileName = "FileName"
)

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
func (m *BPFMap) Features() []state.Feature {
	return nil
}

// Delete removes an entity from the store
func (m *BPFMap) Delete(req *state.DeleteRequest) error {
	return fmt.Errorf("delete operation not supported")
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
	return fmt.Errorf("set operation not supported")
}

// Close implements io.Closer
func (m *BPFMap) Close() error {
	err := m.client.Close()
	if err != nil {
		return err
	}
	return nil
}
