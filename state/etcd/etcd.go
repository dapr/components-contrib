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
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/dapr/components-contrib/internal/utils"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	stateutils "github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

// Etcd is a state store implementation for Etcd.
type Etcd struct {
	state.BulkStore

	client        *clientv3.Client
	keyPrefixPath string
	features      []state.Feature
	logger        logger.Logger
}

type etcdConfig struct {
	Endpoints     string `json:"endpoints"`
	KeyPrefixPath string `json:"keyPrefixPath"`
	// TLS
	TLSEnable string `json:"tlsEnable"`
	CA        string `json:"ca"`
	Cert      string `json:"cert"`
	Key       string `json:"key"`
}

// NewEtcdStateStore returns a new etcd state store.
func NewEtcdStateStore(logger logger.Logger) state.Store {
	s := &Etcd{
		logger:   logger,
		features: []state.Feature{state.FeatureETag, state.FeatureTransactional},
	}
	s.BulkStore = state.NewDefaultBulkStore(s)
	return s
}

// Init does metadata and config parsing and initializes the
// Etcd client.
func (e *Etcd) Init(_ context.Context, metadata state.Metadata) error {
	etcdConfig, err := metadataToConfig(metadata.Properties)
	if err != nil {
		return fmt.Errorf("couldn't convert metadata properties: %w", err)
	}

	e.client, err = e.ParseClientFromConfig(etcdConfig)
	if err != nil {
		return fmt.Errorf("initializing etcd client: %w", err)
	}

	e.keyPrefixPath = etcdConfig.KeyPrefixPath

	return nil
}

func (e *Etcd) ParseClientFromConfig(etcdConfig *etcdConfig) (*clientv3.Client, error) {
	endpoints := strings.Split(etcdConfig.Endpoints, ",")
	if len(endpoints) == 0 || endpoints[0] == "" {
		return nil, errors.New("endpoints required")
	}

	var tlsConfig *tls.Config
	if utils.IsTruthy(etcdConfig.TLSEnable) {
		if etcdConfig.Cert != "" && etcdConfig.Key != "" && etcdConfig.CA != "" {
			var err error
			tlsConfig, err = NewTLSConfig(etcdConfig.Cert, etcdConfig.Key, etcdConfig.CA)
			if err != nil {
				return nil, fmt.Errorf("tls authentication error: %w", err)
			}
		} else {
			return nil, errors.New("tls authentication information is incomplete")
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
		return nil, fmt.Errorf("couldn't get key %s: %w", keyWithPath, err)
	}

	if resp == nil || len(resp.Kvs) == 0 {
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

	keyWithPath := e.keyPrefixPath + "/" + req.Key
	err = e.doValidateEtag(keyWithPath, req.ETag, req.Options.Concurrency)
	if err != nil {
		return err
	}

	reqVal, err := stateutils.Marshal(req.Value, json.Marshal)
	if err != nil {
		return err
	}

	return e.doSet(ctx, keyWithPath, string(reqVal), req.ETag, ttlInSeconds)
}

func (e *Etcd) doSet(ctx context.Context, key, reqVal string, etag *string, ttlInSeconds int64) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if ttlInSeconds > 0 {
		resp, err := e.client.Grant(ctx, ttlInSeconds)
		if err != nil {
			return fmt.Errorf("couldn't grant lease %s: %w", key, err)
		}
		if etag != nil {
			etag, _ := strconv.ParseInt(*etag, 10, 64)
			_, err = e.client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision(key), "=", etag)).
				Then(clientv3.OpPut(key, reqVal, clientv3.WithLease(resp.ID))).
				Commit()
		} else {
			_, err = e.client.Put(ctx, key, reqVal, clientv3.WithLease(resp.ID))
		}
		if err != nil {
			return fmt.Errorf("couldn't set key %s: %w", key, err)
		}
	} else {
		var err error
		if etag != nil {
			etag, _ := strconv.ParseInt(*etag, 10, 64)
			_, err = e.client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision(key), "=", etag)).
				Then(clientv3.OpPut(key, reqVal)).
				Commit()
		} else {
			_, err = e.client.Put(ctx, key, reqVal)
		}
		if err != nil {
			return fmt.Errorf("couldn't set key %s: %w", key, err)
		}
	}
	return nil
}

func (e *Etcd) doSetValidateParameters(req *state.SetRequest) (int64, error) {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return 0, err
	}

	var ttlVal int64
	ttlInSeconds, err := stateutils.ParseTTL(req.Metadata)
	if err != nil {
		return 0, err
	}
	if ttlInSeconds != nil {
		ttlVal = int64(*ttlInSeconds)
	}

	return ttlVal, nil
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

	return e.doDelete(ctx, keyWithPath, req.ETag)
}

func (e *Etcd) doDelete(ctx context.Context, key string, etag *string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var err error
	if etag != nil {
		etag, _ := strconv.ParseInt(*etag, 10, 64)
		_, err = e.client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", etag)).
			Then(clientv3.OpDelete(key)).
			Commit()
	} else {
		_, err = e.client.Delete(ctx, key)
	}
	if err != nil {
		return fmt.Errorf("couldn't delete key %s: %w", key, err)
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
			return fmt.Errorf("couldn't get key %s: %w", key, err)
		} else if item != nil && len(item.Kvs) != 0 {
			return state.NewETagError(state.ETagMismatch, errors.New("item already exists and no etag was passed"))
		} else {
			return nil
		}
	} else if hasEtag {
		item, err := e.client.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("couldn't get key %s: %w", key, err)
		}
		if item == nil || len(item.Kvs) == 0 {
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
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
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
		return fmt.Errorf("etcd store: error connecting to etcd at %s: %w", e.client.Endpoints(), err)
	}

	return nil
}

// Multi performs a transactional operation. succeeds only if all operations succeed, and fails if one or more operations fail.
func (e *Etcd) Multi(ctx context.Context, request *state.TransactionalStateRequest) error {
	if len(request.Operations) == 0 {
		return nil
	}

	ops := make([]clientv3.Op, 0, len(request.Operations))

	for _, o := range request.Operations {
		switch req := o.(type) {
		case state.SetRequest:
			ttlInSeconds, err := e.doSetValidateParameters(&req)
			if err != nil {
				return err
			}

			keyWithPath := e.keyPrefixPath + "/" + req.Key
			err = e.doValidateEtag(keyWithPath, req.ETag, req.Options.Concurrency)
			if err != nil {
				return err
			}

			reqVal, err := stateutils.Marshal(req.Value, json.Marshal)
			if err != nil {
				return err
			}
			var cmp clientv3.Cmp
			if req.HasETag() {
				etag, _ := strconv.ParseInt(*req.ETag, 10, 64)
				cmp = clientv3.Compare(clientv3.ModRevision(keyWithPath), "=", etag)
			}
			if ttlInSeconds > 0 {
				resp, err := e.client.Grant(ctx, ttlInSeconds)
				if err != nil {
					return fmt.Errorf("couldn't grant lease %s: %w", keyWithPath, err)
				}
				put := clientv3.OpPut(keyWithPath, string(reqVal), clientv3.WithLease(resp.ID))
				if req.HasETag() {
					ops = append(ops, clientv3.OpTxn([]clientv3.Cmp{cmp}, []clientv3.Op{put}, nil))
				} else {
					ops = append(ops, clientv3.OpTxn(nil, []clientv3.Op{put}, nil))
				}
			} else {
				put := clientv3.OpPut(keyWithPath, string(reqVal))
				if req.HasETag() {
					ops = append(ops, clientv3.OpTxn([]clientv3.Cmp{cmp}, []clientv3.Op{put}, nil))
				} else {
					ops = append(ops, clientv3.OpTxn(nil, []clientv3.Op{put}, nil))
				}
			}
		case state.DeleteRequest:
			if err := state.CheckRequestOptions(req.Options); err != nil {
				return err
			}

			keyWithPath := e.keyPrefixPath + "/" + req.Key
			err := e.doValidateEtag(keyWithPath, req.ETag, req.Options.Concurrency)
			if err != nil {
				return err
			}

			del := clientv3.OpDelete(keyWithPath)
			if req.HasETag() {
				etag, _ := strconv.ParseInt(*req.ETag, 10, 64)
				cmp := clientv3.Compare(clientv3.ModRevision(keyWithPath), "=", etag)
				ops = append(ops, clientv3.OpTxn([]clientv3.Cmp{cmp}, []clientv3.Op{del}, nil))
			} else {
				ops = append(ops, clientv3.OpTxn(nil, []clientv3.Op{del}, nil))
			}
		}
	}

	_, err := e.client.Txn(ctx).Then(ops...).Commit()
	if err != nil {
		return err
	}

	return nil
}

func NewTLSConfig(clientCert, clientKey, caCert string) (*tls.Config, error) {
	valid := false

	config := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if clientCert != "" && clientKey != "" {
		key := []byte(clientKey)
		cert, err := tls.X509KeyPair([]byte(clientCert), key)
		if err != nil {
			return nil, fmt.Errorf("error parse X509KeyPair: %w", err)
		}
		config.Certificates = []tls.Certificate{cert}
		valid = true
	}

	if caCert != "" {
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM([]byte(caCert))
		config.RootCAs = caCertPool
		valid = true
	}

	if !valid {
		config = nil
	}

	return config, nil
}
