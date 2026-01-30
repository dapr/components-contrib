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

package structuredformat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	yaml "gopkg.in/yaml.v3"

	"github.com/dapr/components-contrib/metadata"
	nr "github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	JSONStructuredValue     = "json"
	YAMLStructuredValue     = "yaml"
	JSONFileStructuredValue = "jsonFile"
	YAMLFileStructuredValue = "yamlFile"
)

var allowedStructuredTypes = []string{
	JSONStructuredValue,
	YAMLStructuredValue,
	JSONFileStructuredValue,
	YAMLFileStructuredValue,
}

// StructuredFormatResolver parses service names from a structured string
// defined in the configuration.
type StructuredFormatResolver struct {
	meta      structuredFormatMetadata
	instances appInstances
	logger    logger.Logger
	rand      *rand.Rand
}

// structuredFormatMetadata represents the structured string (such as JSON or YAML)
// provided in the configuration for name resolution.
type structuredFormatMetadata struct {
	StructuredType string `mapstructure:"structuredType"`
	StringValue    string `mapstructure:"stringValue"`
	FilePath       string `mapstructure:"filePath"`
}

// appInstances stores the relationship between services and their instances.
type appInstances struct {
	AppInstances map[string][]address `json:"appInstances" yaml:"appInstances"`
}

// address contains service instance information.
type address struct {
	Domain string `json:"domain" yaml:"domain"`
	IPv4   string `json:"ipv4" yaml:"ipv4"`
	IPv6   string `json:"ipv6" yaml:"ipv6"`
	Port   int    `json:"port" yaml:"port"`
}

// isValid checks if the address has at least one valid host field.
func (a address) isValid() bool {
	return (a.Domain != "" || a.IPv4 != "" || a.IPv6 != "")
}

// NewResolver creates a new Structured Format resolver.
func NewResolver(logger logger.Logger) nr.Resolver {
	src := rand.NewSource(time.Now().UnixNano())
	return &StructuredFormatResolver{
		logger: logger,
		// gosec is complaining that we are using a non-crypto-safe PRNG.
		// This is fine in this scenario since we are using it only for selecting a random address for load-balancing.
		//nolint:gosec
		rand: rand.New(src),
	}
}

// Init initializes the structured format resolver with the given metadata.
func (r *StructuredFormatResolver) Init(ctx context.Context, metadata nr.Metadata) error {
	var meta structuredFormatMetadata
	if err := kitmd.DecodeMetadata(metadata.Configuration, &meta); err != nil {
		return fmt.Errorf("failed to decode metadata: %w", err)
	}

	// Validate structuredType
	if !isValidStructuredType(meta.StructuredType) {
		return fmt.Errorf("invalid structuredType %q; must be one of: %s",
			meta.StructuredType, strings.Join(allowedStructuredTypes, ", "))
	}

	// Validate required fields based on type
	switch meta.StructuredType {
	case JSONStructuredValue, YAMLStructuredValue:
		if meta.StringValue == "" {
			return fmt.Errorf("stringValue is required when structuredType is %q", meta.StructuredType)
		}
	case JSONFileStructuredValue, YAMLFileStructuredValue:
		if meta.FilePath == "" {
			return fmt.Errorf("filePath is required when structuredType is %q", meta.StructuredType)
		}
	}

	r.meta = meta

	instances, err := loadStructuredFormatData(r)
	if err != nil {
		return fmt.Errorf("failed to load structured data: %w", err)
	}

	// validate that all addresses are valid
	for serviceID, addrs := range instances.AppInstances {
		for i, addr := range addrs {
			if !addr.isValid() {
				return fmt.Errorf("invalid address at AppInstances[%q][%d]: missing domain, ipv4, and ipv6", serviceID, i)
			}
			if addr.Port <= 0 || addr.Port > 65535 {
				return fmt.Errorf("invalid port %d for AppInstances[%q][%d]", addr.Port, serviceID, i)
			}
		}
	}

	r.instances = instances
	return nil
}

// ResolveID resolves a service ID to an address using the configured value.
func (r *StructuredFormatResolver) ResolveID(ctx context.Context, req nr.ResolveRequest) (string, error) {
	if req.ID == "" {
		return "", errors.New("empty ID not allowed")
	}

	addresses, exists := r.instances.AppInstances[req.ID]
	if !exists || len(addresses) == 0 {
		return "", fmt.Errorf("no services found with ID %q", req.ID)
	}

	// Select a random instance (load balancing)
	selected := addresses[r.rand.Intn(len(addresses))]

	// Prefer Domain > IPv4 > IPv6
	host := selected.Domain
	if host == "" {
		host = selected.IPv4
	}
	if host == "" {
		host = selected.IPv6
	}

	// This should not happen due to validation in Init, but be defensive.
	if host == "" {
		return "", fmt.Errorf("resolved address for %q has no valid host", req.ID)
	}

	return net.JoinHostPort(host, strconv.Itoa(selected.Port)), nil
}

// Close implements io.Closer.
func (r *StructuredFormatResolver) Close() error {
	return nil
}

// GetComponentMetadata returns metadata info used for documentation and validation.
func (r *StructuredFormatResolver) GetComponentMetadata() metadata.MetadataMap {
	m := metadata.MetadataMap{}
	metadata.GetMetadataInfoFromStructType(
		reflect.TypeOf(structuredFormatMetadata{}),
		&m,
		metadata.NameResolutionType,
	)
	return m
}

// isValidStructuredType checks if the given type is allowed.
func isValidStructuredType(t string) bool {
	for _, allowed := range allowedStructuredTypes {
		if t == allowed {
			return true
		}
	}
	return false
}

// loadStructuredFormatData loads the mapping from structured input.
func loadStructuredFormatData(r *StructuredFormatResolver) (appInstances, error) {
	var instances appInstances

	var data []byte
	var err error

	switch r.meta.StructuredType {
	case JSONStructuredValue, YAMLStructuredValue:
		data = []byte(r.meta.StringValue)
	case JSONFileStructuredValue, YAMLFileStructuredValue:
		// Security note: Consider restricting file access in production (e.g., allowlist paths).
		data, err = os.ReadFile(r.meta.FilePath)
		if err != nil {
			return instances, fmt.Errorf("failed to read file %q: %w", r.meta.FilePath, err)
		}
	default:
		// Should not happen due to prior validation
		return instances, fmt.Errorf("unsupported structuredType: %s", r.meta.StructuredType)
	}

	// Parse based on format
	switch r.meta.StructuredType {
	case JSONStructuredValue, JSONFileStructuredValue:
		err = json.Unmarshal(data, &instances)
	case YAMLStructuredValue, YAMLFileStructuredValue:
		err = yaml.Unmarshal(data, &instances)
	}

	if err != nil {
		return instances, fmt.Errorf("failed to parse %s data: %w", getFormatName(r.meta.StructuredType), err)
	}

	return instances, nil
}

// getFormatName returns a human-readable format name.
func getFormatName(t string) string {
	switch t {
	case JSONStructuredValue, JSONFileStructuredValue:
		return "JSON"
	case YAMLStructuredValue, YAMLFileStructuredValue:
		return "YAML"
	default:
		return t
	}
}
