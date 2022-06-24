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

package memcached

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	jsoniter "github.com/json-iterator/go"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
)

const (
	hosts              = "hosts"
	maxIdleConnections = "maxIdleConnections"
	timeout            = "timeout"
	ttlInSeconds       = "ttlInSeconds"
	// These defaults are already provided by gomemcache.
	defaultMaxIdleConnections = 2
	defaultTimeout            = 1000 * time.Millisecond
)

type Memcached struct {
	state.DefaultBulkStore
	client *memcache.Client
	json   jsoniter.API
	logger logger.Logger
}

type memcachedMetadata struct {
	hosts              []string
	maxIdleConnections int
	timeout            time.Duration
}

func NewMemCacheStateStore(logger logger.Logger) *Memcached {
	s := &Memcached{
		json:   jsoniter.ConfigFastest,
		logger: logger,
	}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
}

func (m *Memcached) Init(metadata state.Metadata) error {
	meta, err := getMemcachedMetadata(metadata)
	if err != nil {
		return err
	}

	client := memcache.New(meta.hosts...)
	client.Timeout = meta.timeout
	client.MaxIdleConns = meta.maxIdleConnections

	m.client = client

	err = client.Ping()
	if err != nil {
		return err
	}

	return nil
}

// Features returns the features available in this state store.
func (m *Memcached) Features() []state.Feature {
	return nil
}

func getMemcachedMetadata(metadata state.Metadata) (*memcachedMetadata, error) {
	meta := memcachedMetadata{
		maxIdleConnections: defaultMaxIdleConnections,
		timeout:            defaultTimeout,
	}

	if val, ok := metadata.Properties[hosts]; ok && val != "" {
		meta.hosts = strings.Split(val, ",")
	} else {
		return nil, errors.New("missing or empty hosts field from metadata")
	}

	if val, ok := metadata.Properties[maxIdleConnections]; ok && val != "" {
		p, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("error parsing maxIdleConnections")
		}
		meta.maxIdleConnections = p
	}

	if val, ok := metadata.Properties[timeout]; ok && val != "" {
		p, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("error parsing timeout")
		}
		meta.timeout = time.Duration(p) * time.Millisecond
	}

	return &meta, nil
}

func (m *Memcached) parseTTL(req *state.SetRequest) (*int32, error) {
	if val, ok := req.Metadata[ttlInSeconds]; ok && val != "" {
		parsedVal, err := strconv.ParseInt(val, 10, 0)
		if err != nil {
			return nil, err
		}
		parsedInt := int32(parsedVal)

		return &parsedInt, nil
	}

	return nil, nil
}

func (m *Memcached) setValue(req *state.SetRequest) error {
	var bt []byte
	ttl, err := m.parseTTL(req)
	if err != nil {
		return fmt.Errorf("failed to parse ttl %s: %s", req.Key, err)
	}

	bt, _ = utils.Marshal(req.Value, m.json.Marshal)
	if ttl != nil {
		err = m.client.Set(&memcache.Item{Key: req.Key, Value: bt, Expiration: *ttl})
	} else {
		err = m.client.Set(&memcache.Item{Key: req.Key, Value: bt})
	}
	if err != nil {
		return fmt.Errorf("failed to set key %s: %s", req.Key, err)
	}

	return nil
}

func (m *Memcached) Delete(req *state.DeleteRequest) error {
	err := m.client.Delete(req.Key)
	if err != nil {
		return err
	}

	return nil
}

func (m *Memcached) Get(req *state.GetRequest) (*state.GetResponse, error) {
	item, err := m.client.Get(req.Key)
	if err != nil {
		// Return nil for status 204
		if errors.Is(err, memcache.ErrCacheMiss) {
			return &state.GetResponse{}, nil
		}

		return &state.GetResponse{}, err
	}

	return &state.GetResponse{
		Data: item.Value,
	}, nil
}

func (m *Memcached) Set(req *state.SetRequest) error {
	return state.SetWithOptions(m.setValue, req)
}
