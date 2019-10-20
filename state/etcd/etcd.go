// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package etcd

import (
	"time"

	"github.com/dapr/components-contrib/state"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
)

// StateStore is a Etcd state store
type StateStore struct {
	client *clientv3.Client
}

//--- StateStore ---

// Init does metadata and connection parsing
func (r *StateStore) Init(metadata state.Metadata) error {

	endpoints := []string{""}
	dialTimeout := time.Duration(20 * time.Second)

	clientConfig := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
		DialOptions: []grpc.DialOption{grpc.WithBlock()},
	}

	client, err := clientv3.New(clientConfig)
	if err != nil {
		return err
	}

	r.client = client

	return nil
}

// Get retrieves state from redis with a key
func (r *StateStore) Get(req *state.GetRequest) (*state.GetResponse, error) {
	return nil, nil
}

// Delete performs a delete operation
func (r *StateStore) Delete(req *state.DeleteRequest) error {
	return nil
}

// BulkDelete performs a bulk delete operation
func (r *StateStore) BulkDelete(req []state.DeleteRequest) error {
	for _, re := range req {
		err := r.Delete(&re)
		if err != nil {
			return err
		}
	}

	return nil
}

// Set saves state into Etcd
func (r *StateStore) Set(req *state.SetRequest) error {
	return nil
}

// BulkSet performs a bulks save operation
func (r *StateStore) BulkSet(req []state.SetRequest) error {
	for _, s := range req {
		err := r.Set(&s)
		if err != nil {
			return err
		}
	}

	return nil
}

//--- TransactionalStateStore ---

// Multi performs a transactional operation. succeeds only if all operations succeed, and fails if one or more operations fail
func (r *StateStore) Multi(operations []state.TransactionalRequest) error {
	return nil
}
