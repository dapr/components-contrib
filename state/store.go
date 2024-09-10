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
	"io"

	"github.com/dapr/components-contrib/health"
	"github.com/dapr/components-contrib/metadata"
)

// ErrPingNotImplemented is returned by Ping if the state store does not implement the Pinger interface
var ErrPingNotImplemented = errors.New("ping is not implemented by this state store")

// Store is an interface to perform operations on store.
type Store interface {
	metadata.ComponentWithMetadata

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
	io.Closer
}

// TransactionalStore is an interface for initialization and support multiple transactional requests.
type TransactionalStore interface {
	Multi(ctx context.Context, request *TransactionalStateRequest) error
}

// TransactionalStoreMultiMaxSize is an optional interface transactional state stores can implement to indicate the maximum size for a transaction.
type TransactionalStoreMultiMaxSize interface {
	MultiMaxSize() int
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
		return ErrPingNotImplemented
	}
}

// DeleteWithPrefix is an optional interface to delete objects with a prefix.
type DeleteWithPrefix interface {
	DeleteWithPrefix(ctx context.Context, req DeleteWithPrefixRequest) (DeleteWithPrefixResponse, error)
}
