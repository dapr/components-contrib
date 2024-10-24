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
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	jsoniter "github.com/json-iterator/go"
	"k8s.io/utils/clock"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	maxIdleConnections = "maxIdleConnections"
	timeout            = "timeout"
	ttlInSeconds       = "ttlInSeconds"
	// These defaults are already provided by gomemcache.
	defaultMaxIdleConnections = 2
	defaultTimeout            = 1000 * time.Millisecond
)

type Memcached struct {
	state.BulkStore

	client *memcache.Client
	json   jsoniter.API
	logger logger.Logger
	clock  clock.Clock
}

type memcachedMetadata struct {
	Hosts              []string `mapstructure:"hosts"`
	MaxIdleConnections int      `mapstructure:"maxIdleConnections"`
	Timeout            int      `mapstructure:"timeout"`
}

func NewMemCacheStateStore(logger logger.Logger) state.Store {
	s := &Memcached{
		json:   jsoniter.ConfigFastest,
		logger: logger,
		clock:  clock.RealClock{},
	}
	s.BulkStore = state.NewDefaultBulkStore(s)
	return s
}

func (m *Memcached) Init(_ context.Context, metadata state.Metadata) error {
	meta, err := getMemcachedMetadata(metadata)
	if err != nil {
		return err
	}

	client := memcache.New(meta.Hosts...)
	if meta.Timeout < 0 {
		client.Timeout = defaultTimeout
	} else {
		client.Timeout = time.Duration(meta.Timeout) * time.Millisecond
	}
	client.MaxIdleConns = meta.MaxIdleConnections

	m.client = client

	// TODO: pass context when PR is merged.
	// https://github.com/bradfitz/gomemcache/pull/126
	err = client.Ping()
	if err != nil {
		return err
	}

	return nil
}

// Features returns the features available in this state store.
func (m *Memcached) Features() []state.Feature {
	return []state.Feature{
		state.FeatureTTL,
	}
}

func getMemcachedMetadata(meta state.Metadata) (*memcachedMetadata, error) {
	m := memcachedMetadata{
		MaxIdleConnections: defaultMaxIdleConnections,
		Timeout:            -1,
	}

	err := kitmd.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return nil, err
	}

	if m.Hosts == nil || len(m.Hosts) == 0 {
		return nil, errors.New("missing or empty hosts field from metadata")
	}

	if val, ok := meta.Properties[maxIdleConnections]; ok && val != "" {
		p, err := strconv.Atoi(val)
		if err != nil {
			return nil, errors.New("error parsing maxIdleConnections")
		}
		m.MaxIdleConnections = p
	}

	if val, ok := meta.Properties[timeout]; ok && val != "" {
		p, err := strconv.Atoi(val)
		if err != nil {
			return nil, errors.New("error parsing timeout")
		}
		m.Timeout = p
	}

	return &m, nil
}

func (m *Memcached) parseTTL(req *state.SetRequest) (*int32, error) {
	if val, ok := req.Metadata[ttlInSeconds]; ok && val != "" {
		parsedVal, err := strconv.ParseInt(val, 10, 0)
		if err != nil {
			return nil, err
		}

		parsedInt := int32(parsedVal)

		// If ttl is more than 30 days, convert it to unix timestamp.
		// https://github.com/memcached/memcached/wiki/Commands#standard-protocol
		if parsedInt >= 60*60*24*30 {
			parsedInt = int32(m.clock.Now().Unix()) + parsedInt //nolint:gosec
		}

		// Notice that for Dapr, -1 means "persist with no TTL".
		// Memcached uses "0" as the non-expiring marker TTL.
		// https://github.com/memcached/memcached/wiki/Commands#set
		// So let's translate Dapr's -1 and beyound to Memcache's 0
		if parsedInt < 0 {
			parsedInt = 0
		}

		return &parsedInt, nil
	}

	return nil, nil
}

func (m *Memcached) Set(ctx context.Context, req *state.SetRequest) error {
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

func (m *Memcached) Delete(ctx context.Context, req *state.DeleteRequest) error {
	err := m.client.Delete(req.Key)
	if err != nil {
		if err == memcache.ErrCacheMiss {
			return nil
		}
		return err
	}

	return nil
}

func (m *Memcached) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
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

func (m *Memcached) Close() (err error) {
	if m.client != nil {
		err = m.client.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Memcached) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := memcachedMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return
}
