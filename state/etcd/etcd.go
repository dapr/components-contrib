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

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
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
	Ca          string `json:"ca"`
	Cert        string `json:"cert"`
	Key         string `json:"key"`
	KeyPassword string `json:"keyPassword"`
}

// NewEtcdStateStore returns a new etcd state store.
func NewEtcdStateStore(logger logger.Logger) state.Store {
	s := &Etcd{
		logger:   logger,
		features: []state.Feature{state.FeatureTransactional},
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

	endpoints := strings.Split(etcdConfig.Endpoints, ",")
	if len(endpoints) == 0 || endpoints[0] == "" {
		return fmt.Errorf("endpoints required")
	}

	var tlsConfig *tls.Config
	if etcdConfig.Cert != "" && etcdConfig.Key != "" && etcdConfig.Ca != "" {
		tlsConfig, err = utils.NewTLSConfigWithPassword(etcdConfig.Cert, etcdConfig.Key, etcdConfig.KeyPassword, etcdConfig.Ca)
		if err != nil {
			return err
		}
	}
	tlsConfig.InsecureSkipVerify = true
	config := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
		TLS:         tlsConfig,
	}

	client, err := clientv3.New(config)
	if err != nil {
		return errors.Wrap(err, "initializing etcd client")
	}
	e.client = client
	e.keyPrefixPath = etcdConfig.KeyPrefixPath

	return nil
}

// Features returns the features available in this state store.
func (e *Etcd) Features() []state.Feature {
	// Etag is just returned and not handled in set or delete operations.
	return e.features
}

func metadataToConfig(connInfo map[string]string) (*etcdConfig, error) {
	m := &etcdConfig{}
	err := metadata.DecodeMetadata(connInfo, m)
	return m, err
}

// Get retrieves a Etcd KV item.
func (e *Etcd) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := e.client.Get(ctx, e.keyPrefixPath+"/"+req.Key)
	if err != nil {
		return nil, err
	}

	if resp.Kvs == nil {
		return &state.GetResponse{}, nil
	}

	return &state.GetResponse{
		Data: resp.Kvs[0].Value,
		ETag: ptr.Of(strconv.Itoa(int(resp.Kvs[0].Version))),
	}, nil
}

// Set saves a Etcd KV item.
func (e *Etcd) Set(ctx context.Context, req *state.SetRequest) error {
	var reqVal string
	switch obj := req.Value.(type) {
	case []byte:
		reqVal = string(obj)
	case string:
		reqVal = fmt.Sprintf("%s", obj)
	default:
		return fmt.Errorf("request value %v is not valid", reqVal)
	}

	keyWithPath := e.keyPrefixPath + "/" + req.Key

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := e.client.Put(ctx, keyWithPath, reqVal)
	if err != nil {
		return fmt.Errorf("couldn't set key %s: %s", keyWithPath, err)
	}

	return nil
}

// Delete performes a Etcd KV delete operation.
func (e *Etcd) Delete(ctx context.Context, req *state.DeleteRequest) error {
	keyWithPath := e.keyPrefixPath + "/" + req.Key

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := e.client.Delete(ctx, keyWithPath)
	if err != nil {
		return fmt.Errorf("couldn't delete key %s: %s", keyWithPath, err)
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
	ops := make([]clientv3.Op, 0)

	for _, o := range request.Operations {
		if o.Operation == state.Upsert {
			req := o.Request.(state.SetRequest)
			keyWithPath := e.keyPrefixPath + "/" + req.Key
			var v string
			switch x := req.Value.(type) {
			case string:
				v = x
			case []uint8:
				v = string(x)
			default:
				return fmt.Errorf("request value %v is not valid", v)
			}
			ops = append(ops, clientv3.OpPut(keyWithPath, v))
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
