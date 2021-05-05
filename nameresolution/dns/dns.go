// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package dns

import (
	"fmt"
	"strings"

	"github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
)

type resolver struct {
	logger logger.Logger
}

// NewResolver creates DNS name resolver.
func NewResolver(logger logger.Logger) nameresolution.Resolver {
	return &resolver{logger: logger}
}

// Init initializes DNS name resolver.
func (k *resolver) Init(metadata nameresolution.Metadata) error {
	return nil
}

// ResolveID resolves name to address use DNS
func (k *resolver) ResolveID(req nameresolution.ResolveRequest) (string, error) {
	if strings.TrimSpace(req.Namespace) != "" {
		return fmt.Sprintf("%s-dapr.%s.svc:%d", req.ID, req.Namespace, req.Port), nil
	}
	
	return fmt.Sprintf("%s-dapr:%d", req.ID, req.Port), nil
}
