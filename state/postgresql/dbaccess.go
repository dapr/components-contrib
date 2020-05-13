// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package postgresql

import (
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/dapr/pkg/logger"
)

type dbAccess interface {
	Logger() logger.Logger
	Init(metadata *state.Metadata) (error)
	Set(req *state.SetRequest) (error)
	Get(req *state.GetRequest) (*state.GetResponse, error)
}