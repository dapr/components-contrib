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

	yaml "gopkg.in/yaml.v3"

	"github.com/dapr/components-contrib/metadata"
	nr "github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	JSONStringStructuredValue = "jsonString"
	YAMLStringStructuredValue = "yamlString"
	JSONFileStructuredValue   = "jsonFile"
	YAMLFileStructuredValue   = "yamlFile"
)

var allowedStructuredTypes = []string{
	JSONStringStructuredValue,
	YAMLStringStructuredValue, JSONFileStructuredValue, YAMLFileStructuredValue,
}

// StructuredFormatResolver parses service names from a structured string
// defined in the configuration.
type StructuredFormatResolver struct {
	meta      structuredFormatMetadata
	instances appInstances

	logger logger.Logger
}

// structuredFormatMetadata represents the structured string (such as JSON or YAML)
// provided in the configuration for name resolution.
type structuredFormatMetadata struct {
	StructuredType string
	StringValue    string
	FilePath       string
}

// appInstances stores the relationship between services and their instances.
type appInstances struct {
	AppInstances map[string][]address `json:"appInstances" yaml:"appInstances"`
}

// address contains service instance information, including Domain, IPv4, IPv6, Port,
//
//	and ExtendedInfo.
type address struct {
	Domain       string            `json:"domain" yaml:"domain"`
	IPV4         string            `json:"ipv4" yaml:"ipv4"`
	IPV6         string            `json:"ipv6" yaml:"ipv6"`
	Port         int               `json:"port" yaml:"port"`
	ExtendedInfo map[string]string `json:"extendedInfo" yaml:"extendedInfo"`
}

// NewResolver creates a new Structured Format resolver.
func NewResolver(logger logger.Logger) nr.Resolver {
	return &StructuredFormatResolver{
		logger: logger,
	}
}

// Init initializes the structured format resolver with the given metadata.
func (r *StructuredFormatResolver) Init(ctx context.Context, metadata nr.Metadata) error {
	var meta structuredFormatMetadata
	err := kitmd.DecodeMetadata(metadata.Configuration, &meta)
	if err != nil {
		return fmt.Errorf("failed to decode metadata: %w", err)
	}

	switch meta.StructuredType {
	case JSONStringStructuredValue, YAMLStringStructuredValue:
		if meta.StringValue == "" {
			return fmt.Errorf("structuredType = %s, stringValue must be not empty", meta.StructuredType)
		}
	case JSONFileStructuredValue, YAMLFileStructuredValue:
		if meta.FilePath == "" {
			return fmt.Errorf("structuredType = %s, filePath must be not empty", meta.StructuredType)
		}
	default:
		return fmt.Errorf("structuredType must be one of: %s",
			strings.Join(allowedStructuredTypes, ", "))
	}

	r.meta = meta

	instances, err := loadStructuredFormatData(r)
	if err != nil {
		return err
	}
	r.instances = instances

	return nil
}

// ResolveID resolves a service ID to an address using the configured value.
func (r *StructuredFormatResolver) ResolveID(ctx context.Context, req nr.ResolveRequest) (string, error) {
	if req.ID == "" {
		return "", errors.New("empty ID not allowed")
	}

	if addresses, exists := r.instances.AppInstances[req.ID]; exists && len(addresses) > 0 {
		// gosec is complaining that we are using a non-crypto-safe PRNG. This is fine in this scenario since we are using it only for selecting a random address for load-balancing.
		//nolint:gosec
		address := addresses[rand.Int()%len(addresses)]

		net.JoinHostPort(address.Domain, strconv.Itoa(address.Port))
		if address.Domain != "" {
			return net.JoinHostPort(address.Domain, strconv.Itoa(address.Port)), nil
		} else if address.IPV4 != "" {
			return net.JoinHostPort(address.IPV4, strconv.Itoa(address.Port)), nil
		} else if address.IPV6 != "" {
			return net.JoinHostPort(address.IPV6, strconv.Itoa(address.Port)), nil
		}
	}

	return "", fmt.Errorf("no services found with AppID '%s'", req.ID)
}

// Close implements io.Closer
func (r *StructuredFormatResolver) Close() error {
	return nil
}

// GetComponentMetadata returns the metadata information for the component.
func (r *StructuredFormatResolver) GetComponentMetadata() metadata.MetadataMap {
	metadataInfo := metadata.MetadataMap{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(structuredFormatMetadata{}),
		&metadataInfo, metadata.NameResolutionType)
	return metadataInfo
}

// loadStructuredFormatData loads the mapping between services and their instances from a configuration file.
func loadStructuredFormatData(r *StructuredFormatResolver) (appInstances, error) {
	var instances appInstances
	switch r.meta.StructuredType {
	case JSONStringStructuredValue:
		err := json.Unmarshal([]byte(r.meta.StringValue), &instances)
		if err != nil {
			return instances, err
		}
	case YAMLStringStructuredValue:
		err := yaml.Unmarshal([]byte(r.meta.StringValue), &instances)
		if err != nil {
			return instances, err
		}
	case JSONFileStructuredValue:
		data, err := os.ReadFile(r.meta.FilePath)
		if err != nil {
			return instances, fmt.Errorf("error reading file: %s", err)
		}

		err = json.Unmarshal(data, &instances)
		if err != nil {
			return instances, err
		}
	case YAMLFileStructuredValue:
		data, err := os.ReadFile(r.meta.FilePath)
		if err != nil {
			return instances, fmt.Errorf("error reading file: %s", err)
		}

		err = yaml.Unmarshal(data, &instances)
		if err != nil {
			return instances, err
		}
	default:
		return instances, fmt.Errorf("structuredType must be one of: %s",
			strings.Join(allowedStructuredTypes, ", "))
	}
	return instances, nil
}
