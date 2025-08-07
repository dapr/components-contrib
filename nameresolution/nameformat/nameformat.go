/*
Copyright 2025 The Dapr Authors
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

package nameformat

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/dapr/components-contrib/metadata"
	nr "github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

// NameFormatResolver implements a name resolution component that formats service names
// according to a configurable pattern.
type NameFormatResolver struct {
	format string
	logger logger.Logger
}

// nameFormatMetadata defines the metadata properties for the name format resolver.
type nameFormatMetadata struct {
	Format string `mapstructure:"format" description:"Format string for name resolution. Must contain {appid} placeholder."`
}

// NewResolver creates a new Name Format resolver.
func NewResolver(logger logger.Logger) nr.Resolver {
	return &NameFormatResolver{
		logger: logger,
	}
}

// Init initializes the name format resolver with the given metadata.
func (r *NameFormatResolver) Init(ctx context.Context, metadata nr.Metadata) error {
	var meta nameFormatMetadata
	err := kitmd.DecodeMetadata(metadata.Configuration, &meta)
	if err != nil {
		return fmt.Errorf("failed to decode metadata: %w", err)
	}

	if meta.Format == "" {
		return errors.New("format is required in metadata")
	}

	// Validate that the format contains the appid placeholder
	if !strings.Contains(meta.Format, "{appid}") {
		return errors.New("format must contain {appid} placeholder")
	}

	// Store the format string
	r.format = meta.Format
	r.logger.Debugf("Initialized with format: %s", r.format)

	return nil
}

// ResolveID resolves a service ID to an address using the configured format.
func (r *NameFormatResolver) ResolveID(ctx context.Context, req nr.ResolveRequest) (string, error) {
	if req.ID == "" {
		return "", errors.New("empty ID not allowed")
	}

	// Replace {appid} with the actual ID
	resolvedAddress := strings.ReplaceAll(r.format, "{appid}", req.ID)
	r.logger.Debugf("Resolved app ID '%s' to address: %s", req.ID, resolvedAddress)
	return resolvedAddress, nil
}

// Close implements io.Closer
func (r *NameFormatResolver) Close() error {
	return nil
}

// GetComponentMetadata returns the metadata information for the component.
func (r *NameFormatResolver) GetComponentMetadata() metadata.MetadataMap {
	metadataInfo := metadata.MetadataMap{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(nameFormatMetadata{}), &metadataInfo, metadata.NameResolutionType)
	return metadataInfo
}
