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

package state

import (
	"context"
	"errors"

	"github.com/dapr/components-contrib/health"
)

// Store is an interface to perform operations on store.
type Store interface {
	BaseStore
	BulkStore
}

// BaseStore is an interface that contains the base methods for each state store.
type BaseStore interface {
	Init(ctx context.Context, metadata Metadata) error
	Features() []Feature
	Delete(ctx context.Context, req *DeleteRequest) error
	Get(ctx context.Context, req *GetRequest) (*GetResponse, error)
	Set(ctx context.Context, req *SetRequest) error
	GetComponentMetadata() map[string]string
}

// TransactionalStore is an interface for initialization and support multiple transactional requests.
type TransactionalStore interface {
	Multi(ctx context.Context, request *TransactionalStateRequest) error
}

// Querier is an interface to execute queries.
type Querier interface {
	Query(ctx context.Context, req *QueryRequest) (*QueryResponse, error)
}

func Ping(ctx context.Context, store Store) error {
	// checks if this store has the ping option then executes
	if storeWithPing, ok := store.(health.Pinger); ok {
		return storeWithPing.Ping(ctx)
	} else {
		return errors.New("ping is not implemented by this state store")
	}
}
