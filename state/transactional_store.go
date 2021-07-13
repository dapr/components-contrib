// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package state

// TransactionalStore is an interface for initialization and support multiple transactional requests.
type TransactionalStore interface {
	Init(metadata Metadata) error
	Multi(request *TransactionalStateRequest) error
}
