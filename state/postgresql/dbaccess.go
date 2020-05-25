// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package postgresql

import (
	"github.com/dapr/components-contrib/state"
	//"github.com/dapr/dapr/pkg/logger"
)

// dbAccess is a private interface which enables unit testing of PostgreSQL
type dbAccess interface {
	Init(metadata state.Metadata) (error)
	Set(req *state.SetRequest) (error)
	Get(req *state.GetRequest) (*state.GetResponse, error)
	Delete(req *state.DeleteRequest) error
	Close() error // Implements io.Closer
}