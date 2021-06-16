// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mns

import (
	"context"

	"github.com/dapr/components-contrib/internal/retry"
	"github.com/dapr/kit/logger"
)

type mns struct {
	name     string
	settings Settings
	logger   logger.Logger
	topics   map[string]bool

	ctx           context.Context
	cancel        context.CancelFunc
	backOffConfig retry.Config
}
