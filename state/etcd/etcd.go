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

package etcd

import (
	"context"
	"crypto/tls"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

const (
	tlsEnable = "enable"
)

// Etcd is a state store implementation for Etcd.
type Etcd struct {
	state.DefaultBulkStore
	client        *clientv3.Client
	keyPrefixPath string
	features      []state.Feature
	logger        logger.Logger
}

type etcdConfig struct {
	Endpoints     string `json:"endpoints"`
	KeyPrefixPath string `json:"keyPrefixPath"`
	// TLS
	TlsEnable   string `json:"tlsEnable"`
	Ca          string `json:"ca"`
	Cert        string `json:"cert"`
	Key         string `json:"key"`
	KeyPassword string `json:"keyPassword"`
}

// NewEtcdStateStore returns a new etcd state store.
func NewEtcdStateStore(logger logger.Logger) state.Store {
	s := &Etcd{
		logger:   logger,
		features: []state.Feature{state.FeatureETag, state.FeatureTransactional},
	}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
}

// Init does metadata and config parsing and initializes the
// Etcd client.
func (e *Etcd) Init(metadata state.Metadata) error {
	etcdConfig, err := metadataToConfig(metadata.Properties)
	if err != nil {
		return fmt.Errorf("couldn't convert metadata properties: %s", err)
	}

	e.client, err = e.ParseClientFromConfig(etcdConfig)
	if err != nil {
		return fmt.Errorf("initializing etcd client: %s", err)
	}

	e.keyPrefixPath = etcdConfig.KeyPrefixPath

	return nil
}

func (e *Etcd) ParseClientFromConfig(etcdConfig *etcdConfig) (*clientv3.Client, error) {
	endpoints := strings.Split(etcdConfig.Endpoints, ",")
	if len(endpoints) == 0 || endpoints[0] == "" {
		return nil, fmt.Errorf("endpoints required")
	}

	var tlsConfig *tls.Config
	if etcdConfig.TlsEnable == tlsEnable {
		if etcdConfig.Cert != "" && etcdConfig.Key != "" && etcdConfig.Ca != "" {
			var err error
			tlsConfig, err = utils.NewTLSConfigWithPassword(etcdConfig.Cert, etcdConfig.Key, etcdConfig.KeyPassword, etcdConfig.Ca)
			if err != nil {
				return nil, fmt.Errorf("tls authentication error: %s", err)
			}
		} else {
			return nil, fmt.Errorf("tls authentication information is incomplete")
		}
	}

	config := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
		TLS:         tlsConfig,
	}
	client, err := clientv3.New(config)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// Features returns the features available in this state store.
func (e *Etcd) Features() []state.Feature {
	return e.features
}

func metadataToConfig(connInfo map[string]string) (*etcdConfig, error) {
	m := &etcdConfig{}
	err := metadata.DecodeMetadata(connInfo, m)
	return m, err
}

// Get retrieves a Etcd KV item.
func (e *Etcd) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	keyWithPath := e.keyPrefixPath + "/" + req.Key

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := e.client.Get(ctx, keyWithPath)
	if err != nil {
		return nil, fmt.Errorf("couldn't get key %s: %s", keyWithPath, err)
	}

	if resp == nil || resp.Kvs == nil {
		return &state.GetResponse{}, nil
	}

	return &state.GetResponse{
		Data: resp.Kvs[0].Value,
		ETag: ptr.Of(strconv.Itoa(int(resp.Kvs[0].ModRevision))),
	}, nil
}

// Set saves a Etcd KV item.
func (e *Etcd) Set(ctx context.Context, req *state.SetRequest) error {
	ttlInSeconds, err := e.doSetValidateParameters(req)
	if err != nil {
		return err
	}

	err = e.doValidateEtag(req.Key, req.ETag, req.Options.Concurrency)
	if err != nil {
		return err
	}

	reqVal, err := e.marshal(req.Value)
	if err != nil {
		return err
	}

	keyWithPath := e.keyPrefixPath + "/" + req.Key
	return e.doSet(ctx, keyWithPath, reqVal, ttlInSeconds)
}

func (e *Etcd) BulkSet(ctx context.Context, req []state.SetRequest) error {
	if len(req) == 0 {
		return nil
	}

	ttls := make([]int, 0)
	for i := 0; i < len(req); i++ {
		ttlInSeconds, err := e.doSetValidateParameters(&req[i])
		if err != nil {
			return err
		}
		ttls = append(ttls, ttlInSeconds)
	}

	for _, dr := range req {
		keyWithPath := e.keyPrefixPath + "/" + dr.Key
		err := e.doValidateEtag(keyWithPath, dr.ETag, dr.Options.Concurrency)
		if err != nil {
			return err
		}
	}

	for i, dr := range req {
		reqVal, err := e.marshal(dr.Value)
		if err != nil {
			return err
		}

		keyWithPath := e.keyPrefixPath + "/" + dr.Key
		err = e.doSet(ctx, keyWithPath, reqVal, ttls[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Etcd) marshal(v interface{}) (string, error) {
	var reqVal string
	switch obj := v.(type) {
	case []byte:
		reqVal = string(obj)
	case string:
		reqVal = fmt.Sprintf("%s", obj)
	default:
		return "", fmt.Errorf("request value %v is not valid", reqVal)
	}
	return reqVal, nil
}

func (e *Etcd) doSet(ctx context.Context, key, reqVal string, ttlInSeconds int) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if ttlInSeconds > 0 {
		resp, err := e.client.Grant(ctx, int64(ttlInSeconds))
		if err != nil {
			return fmt.Errorf("couldn't grant lease %s: %s", key, err)
		}

		_, err = e.client.Put(ctx, key, reqVal, clientv3.WithLease(resp.ID))
		if err != nil {
			return fmt.Errorf("couldn't set key %s: %s", key, err)
		}
	} else {
		_, err := e.client.Put(ctx, key, reqVal)
		if err != nil {
			return fmt.Errorf("couldn't set key %s: %s", key, err)
		}
	}
	return nil
}

func (e *Etcd) doSetValidateParameters(req *state.SetRequest) (int, error) {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return 0, err
	}

	ttlInSeconds, err := doParseTTLInSeconds(req.Metadata)
	if err != nil {
		return 0, err
	}

	return ttlInSeconds, nil
}

func doParseTTLInSeconds(metadata map[string]string) (int, error) {
	s := metadata["ttlInSeconds"]
	if s == "" {
		return 0, nil
	}

	i, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}

	if i < 0 {
		i = 0
	}

	return i, nil
}

// Delete performes a Etcd KV delete operation.
func (e *Etcd) Delete(ctx context.Context, req *state.DeleteRequest) error {
	if err := state.CheckRequestOptions(req.Options); err != nil {
		return err
	}

	keyWithPath := e.keyPrefixPath + "/" + req.Key

	if err := e.doValidateEtag(keyWithPath, req.ETag, req.Options.Concurrency); err != nil {
		return err
	}

	return e.doDelete(ctx, keyWithPath)
}

func (e *Etcd) BulkDelete(ctx context.Context, req []state.DeleteRequest) error {
	if len(req) == 0 {
		return nil
	}

	for i := 0; i < len(req); i++ {
		if err := state.CheckRequestOptions(&req[i].Options); err != nil {
			return err
		}
	}

	for _, dr := range req {
		keyWithPath := e.keyPrefixPath + "/" + dr.Key
		err := e.doValidateEtag(keyWithPath, dr.ETag, dr.Options.Concurrency)
		if err != nil {
			return err
		}
	}

	for _, dr := range req {
		keyWithPath := e.keyPrefixPath + "/" + dr.Key
		err := e.doDelete(ctx, keyWithPath)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Etcd) doDelete(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := e.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("couldn't delete key %s: %s", key, err)
	}
	return nil
}

func (e *Etcd) doValidateEtag(key string, etag *string, concurrency string) error {
	hasEtag := etag != nil && *etag != ""
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if concurrency == state.FirstWrite && !hasEtag {
		item, err := e.client.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("couldn't get key %s: %s", key, err)
		} else if item != nil {
			return state.NewETagError(state.ETagMismatch, fmt.Errorf("item already exists and no etag was passed"))
		} else {
			return nil
		}
	} else if hasEtag {
		item, err := e.client.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("couldn't get key %s: %s", key, err)
		}
		if item == nil || item.Kvs == nil {
			return state.NewETagError(state.ETagMismatch, fmt.Errorf("state not exist or expired for key=%s", key))
		}
		currentEtag := strconv.Itoa(int(item.Kvs[0].ModRevision))
		if currentEtag != *etag {
			return state.NewETagError(state.ETagMismatch, fmt.Errorf(
				"state etag not match for key=%s: current=%s, expect=%s", key, currentEtag, *etag))
		}
	}
	return nil
}

func (e *Etcd) GetComponentMetadata() map[string]string {
	metadataStruct := etcdConfig{}
	metadataInfo := map[string]string{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo)
	return metadataInfo
}

func (e *Etcd) Close() error {
	if e.client == nil {
		return nil
	}

	return e.client.Close()
}

func (e *Etcd) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := e.client.Get(ctx, "health"); err != nil {
		return fmt.Errorf("etcd store: error connecting to etcd at %s: %s", e.client.Endpoints(), err)
	}

	return nil
}

// Multi performs a transactional operation. succeeds only if all operations succeed, and fails if one or more operations fail.
func (e *Etcd) Multi(ctx context.Context, request *state.TransactionalStateRequest) error {
	if len(request.Operations) == 0 {
		return nil
	}

	ops := make([]clientv3.Op, 0)
	ttls := make([]int, 0)
	for _, o := range request.Operations {
		if o.Operation == state.Upsert {
			req := o.Request.(state.SetRequest)
			ttlInSeconds, err := e.doSetValidateParameters(&req)
			if err != nil {
				return err
			}
			ttls = append(ttls, ttlInSeconds)
		} else if o.Operation == state.Delete {
			req := o.Request.(state.DeleteRequest)
			if err := state.CheckRequestOptions(req.Options); err != nil {
				return err
			}
			ttls = append(ttls, 0)
		}
	}

	for _, o := range request.Operations {
		if o.Operation == state.Upsert {
			req := o.Request.(state.SetRequest)
			err := e.doValidateEtag(req.Key, req.ETag, req.Options.Concurrency)
			if err != nil {
				return err
			}
		} else if o.Operation == state.Delete {
			req := o.Request.(state.DeleteRequest)
			err := e.doValidateEtag(req.Key, req.ETag, req.Options.Concurrency)
			if err != nil {
				return err
			}
		}
	}

	for _, o := range request.Operations {
		if o.Operation == state.Upsert {
			req := o.Request.(state.SetRequest)
			keyWithPath := e.keyPrefixPath + "/" + req.Key
			reqVal, err := e.marshal(req.Value)
			if err != nil {
				return err
			}
			ops = append(ops, clientv3.OpPut(keyWithPath, reqVal))
		} else if o.Operation == state.Delete {
			req := o.Request.(state.DeleteRequest)
			keyWithPath := e.keyPrefixPath + "/" + req.Key
			ops = append(ops, clientv3.OpDelete(keyWithPath))
		}
	}

	_, err := e.client.Txn(ctx).Then(ops...).Commit()
	if err != nil {
		return err
	}

	return nil
}
