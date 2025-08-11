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

package zookeeper

import (
	"context"
	"errors"
	"path"
	reflect "reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
	jsoniter "github.com/json-iterator/go"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
	"github.com/dapr/kit/ptr"
)

const (
	anyVersion               = -1
	defaultMaxBufferSize     = 1024 * 1024
	defaultMaxConnBufferSize = 1024 * 1024
)

var (
	errMissingServers        = errors.New("servers are required")
	errInvalidSessionTimeout = errors.New("sessionTimeout is invalid")
)

type properties struct {
	Servers           string `json:"servers"`
	SessionTimeout    string `json:"sessionTimeout"`
	MaxBufferSize     int    `json:"maxBufferSize"`
	MaxConnBufferSize int    `json:"maxConnBufferSize"`
	KeyPrefixPath     string `json:"keyPrefixPath"`
}

type config struct {
	servers           []string
	sessionTimeout    time.Duration
	maxBufferSize     int
	maxConnBufferSize int
	keyPrefixPath     string
}

func newConfig(meta map[string]string) (c *config, err error) {
	var props properties
	errDecode := kitmd.DecodeMetadata(meta, &props)
	if errDecode != nil {
		return nil, errDecode
	}

	return props.parse()
}

func (props *properties) parse() (*config, error) {
	if len(props.Servers) == 0 {
		return nil, errMissingServers
	}

	sessionTimeout, err := time.ParseDuration(props.SessionTimeout)
	if err != nil {
		return nil, errInvalidSessionTimeout
	}

	maxBufferSize := defaultMaxBufferSize
	if props.MaxBufferSize > 0 {
		maxBufferSize = props.MaxBufferSize
	}

	maxConnBufferSize := defaultMaxConnBufferSize
	if props.MaxConnBufferSize > 0 {
		maxConnBufferSize = props.MaxConnBufferSize
	}

	return &config{
		servers:           strings.Split(props.Servers, ","),
		sessionTimeout:    sessionTimeout,
		maxBufferSize:     maxBufferSize,
		maxConnBufferSize: maxConnBufferSize,
		keyPrefixPath:     props.KeyPrefixPath,
	}, nil
}

type Conn interface {
	Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error)

	Get(path string) ([]byte, *zk.Stat, error)

	Set(path string, data []byte, version int32) (*zk.Stat, error)

	Delete(path string, version int32) error

	Multi(ops ...interface{}) ([]zk.MultiResponse, error)
}

//--- StateStore ---

// StateStore is a state store.
type StateStore struct {
	state.BulkStore

	*config
	conn Conn

	features []state.Feature
	logger   logger.Logger
}

// NewZookeeperStateStore returns a new Zookeeper state store.
func NewZookeeperStateStore(logger logger.Logger) state.Store {
	s := &StateStore{
		features: []state.Feature{state.FeatureETag},
		logger:   logger,
	}
	s.BulkStore = state.NewDefaultBulkStore(s)
	return s
}

func (s *StateStore) Init(_ context.Context, metadata state.Metadata) (err error) {
	var c *config

	if c, err = newConfig(metadata.Properties); err != nil {
		return
	}

	conn, _, err := zk.Connect(c.servers, c.sessionTimeout,
		zk.WithMaxBufferSize(c.maxBufferSize), zk.WithMaxConnBufferSize(c.maxConnBufferSize))
	if err != nil {
		return
	}

	s.config = c
	s.conn = conn

	return
}

// Features returns the features available in this state store.
func (s *StateStore) Features() []state.Feature {
	return s.features
}

// Get retrieves state from Zookeeper with a key.
func (s *StateStore) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	value, stat, err := s.conn.Get(s.prefixedKey(req.Key))
	if err != nil {
		if errors.Is(err, zk.ErrNoNode) {
			return &state.GetResponse{}, nil
		}

		return nil, err
	}

	return &state.GetResponse{
		Data: value,
		ETag: ptr.Of(strconv.Itoa(int(stat.Version))),
	}, nil
}

// Delete performs a delete operation.
func (s *StateStore) Delete(ctx context.Context, req *state.DeleteRequest) error {
	r, err := s.newDeleteRequest(req)
	if err != nil {
		return err
	}

	err = s.conn.Delete(r.Path, r.Version)
	if errors.Is(err, zk.ErrNoNode) {
		return nil
	}

	if err != nil {
		if req.HasETag() {
			return state.NewETagError(state.ETagMismatch, err)
		}

		return err
	}

	return nil
}

// Set saves state into Zookeeper.
func (s *StateStore) Set(ctx context.Context, req *state.SetRequest) error {
	r, err := s.newSetDataRequest(req)
	if err != nil {
		return err
	}

	_, err = s.conn.Set(r.Path, r.Data, r.Version)
	if errors.Is(err, zk.ErrNoNode) {
		_, err = s.conn.Create(r.Path, r.Data, 0, nil)
	}

	if err != nil {
		if req.HasETag() {
			return state.NewETagError(state.ETagMismatch, err)
		}

		return err
	}

	return nil
}

func (s *StateStore) newDeleteRequest(req *state.DeleteRequest) (*zk.DeleteRequest, error) {
	err := state.CheckRequestOptions(req)
	if err != nil {
		return nil, err
	}

	var version int32

	if req.Options.Concurrency == state.LastWrite {
		version = anyVersion
	} else {
		var etag string

		if req.HasETag() {
			etag = *req.ETag
		}
		version = s.parseETag(etag)
	}

	return &zk.DeleteRequest{
		Path:    s.prefixedKey(req.Key),
		Version: version,
	}, nil
}

func (s *StateStore) newSetDataRequest(req *state.SetRequest) (*zk.SetDataRequest, error) {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return nil, err
	}

	data, err := s.marshalData(req.Value)
	if err != nil {
		return nil, err
	}

	var version int32

	if req.Options.Concurrency == state.LastWrite {
		version = anyVersion
	} else {
		var etag string

		if req.HasETag() {
			etag = *req.ETag
		}
		version = s.parseETag(etag)
	}

	return &zk.SetDataRequest{
		Path:    s.prefixedKey(req.Key),
		Data:    data,
		Version: version,
	}, nil
}

func (s *StateStore) prefixedKey(key string) string {
	if s.config == nil {
		return key
	}

	return path.Join(s.keyPrefixPath, key)
}

func (s *StateStore) parseETag(etag string) int32 {
	if etag != "" {
		// Since the version is taken to be int32
		version, err := strconv.ParseInt(etag, 10, 32)
		if err == nil {
			return int32(version)
		}
	}

	return anyVersion
}

func (s *StateStore) marshalData(v interface{}) ([]byte, error) {
	if buf, ok := v.([]byte); ok {
		return buf, nil
	}

	return jsoniter.ConfigFastest.Marshal(v)
}

func (s *StateStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := properties{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return
}

func (s *StateStore) Close() error {
	return nil
}
