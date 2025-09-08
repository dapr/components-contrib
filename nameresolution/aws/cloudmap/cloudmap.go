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

package cloudmap

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/servicediscovery"
	"github.com/aws/aws-sdk-go-v2/service/servicediscovery/types"

	awsAuth "github.com/dapr/components-contrib/common/authentication/aws"
	"github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	defaultDaprPort = 3500
)

// ServiceDiscoveryClient interface for mocking
type ServiceDiscoveryClient interface {
	GetNamespace(ctx context.Context, input *servicediscovery.GetNamespaceInput, opts ...func(*servicediscovery.Options)) (*servicediscovery.GetNamespaceOutput, error)
	ListNamespaces(ctx context.Context, input *servicediscovery.ListNamespacesInput, opts ...func(*servicediscovery.Options)) (*servicediscovery.ListNamespacesOutput, error)
	DiscoverInstances(ctx context.Context, input *servicediscovery.DiscoverInstancesInput, opts ...func(*servicediscovery.Options)) (*servicediscovery.DiscoverInstancesOutput, error)
}

// Resolver is the AWS CloudMap name resolver.
type Resolver struct {
	authProvider    awsAuth.Provider
	client          ServiceDiscoveryClient
	logger          logger.Logger
	namespaceID     string
	namespaceName   string
	defaultDaprPort int
}

// NewResolver creates a new AWS CloudMap name resolver.
func NewResolver(logger logger.Logger) nameresolution.Resolver {
	return &Resolver{
		logger:          logger,
		defaultDaprPort: defaultDaprPort,
	}
}

// Init initializes the AWS CloudMap name resolver.
func (r *Resolver) Init(ctx context.Context, metadata nameresolution.Metadata) error {
	var meta cloudMapMetadata
	err := kitmd.DecodeMetadata(metadata.Configuration, &meta)
	if err != nil {
		return fmt.Errorf("failed to decode metadata: %w", err)
	}

	if err = meta.Validate(); err != nil {
		return fmt.Errorf("invalid metadata: %w", err)
	}

	// Set default Dapr port if specified
	if meta.DefaultDaprPort > 0 {
		r.defaultDaprPort = meta.DefaultDaprPort
	}

	// Initialize AWS auth provider
	opts := awsAuth.Options{
		Logger:       r.logger,
		Region:       meta.Region,
		Endpoint:     meta.Endpoint,
		AccessKey:    meta.AccessKey,
		SecretKey:    meta.SecretKey,
		SessionToken: meta.SessionToken,
	}
	cfg := awsAuth.GetConfig(opts)
	provider, err := awsAuth.NewProvider(ctx, opts, cfg)
	if err != nil {
		return fmt.Errorf("failed to create AWS provider: %w", err)
	}
	r.authProvider = provider

	// Create AWS SDK v2 config
	awsCfg, err := awsAuth.GetConfigV2(meta.AccessKey, meta.SecretKey, meta.SessionToken, meta.Region, meta.Endpoint)
	if err != nil {
		return fmt.Errorf("failed to create AWS config: %w", err)
	}

	// Create CloudMap client if not already set (for testing)
	if r.client == nil {
		r.client = servicediscovery.NewFromConfig(awsCfg)
	}

	// Set namespace info
	r.namespaceID = meta.NamespaceID
	r.namespaceName = meta.NamespaceName

	// Validate access to CloudMap and resolve namespace if needed
	if err := r.validateAccess(ctx); err != nil {
		return fmt.Errorf("failed to validate CloudMap access: %w", err)
	}

	return nil
}

// ResolveID resolves a service ID to an address using AWS CloudMap.
func (r *Resolver) ResolveID(ctx context.Context, req nameresolution.ResolveRequest) (string, error) {
	addresses, err := r.resolveIDMulti(ctx, req)
	if err != nil {
		return "", err
	}
	if len(addresses) == 0 {
		return "", fmt.Errorf("no healthy instances found for service: %s", req.ID)
	}
	
	// Return a random address from the list
	//nolint:gosec
	return addresses[rand.Intn(len(addresses))], nil
}

// ResolveIDMulti resolves a service ID to multiple addresses using AWS CloudMap.
func (r *Resolver) ResolveIDMulti(ctx context.Context, req nameresolution.ResolveRequest) (nameresolution.AddressList, error) {
	addresses, err := r.resolveIDMulti(ctx, req)
	if err != nil {
		return nil, err
	}
	return nameresolution.AddressList(addresses), nil
}

func (r *Resolver) resolveIDMulti(ctx context.Context, req nameresolution.ResolveRequest) ([]string, error) {
	input := &servicediscovery.DiscoverInstancesInput{
		NamespaceName: aws.String(r.namespaceName),
		ServiceName:   aws.String(req.ID),
		HealthStatus:  types.HealthStatusFilterHealthy,
	}

	result, err := r.client.DiscoverInstances(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to discover instances for service %s: %w", req.ID, err)
	}

	var addresses []string
	for _, instance := range result.Instances {
		address, err := r.extractInstanceAddress(instance, req.Port)
		if err != nil {
			r.logger.Warnf("Failed to extract address from instance %s: %v", aws.ToString(instance.InstanceId), err)
			continue
		}
		addresses = append(addresses, address)
	}

	r.logger.Debugf("Resolved service %s to %d addresses", req.ID, len(addresses))
	return addresses, nil
}

func (r *Resolver) extractInstanceAddress(instance types.HttpInstanceSummary, requestedPort int) (string, error) {
	var ip string
	var port int

	// Extract IP address (prefer IPv4, fallback to IPv6 or CNAME)
	if instance.Attributes != nil {
		if ipv4, ok := instance.Attributes["AWS_INSTANCE_IPV4"]; ok {
			ip = ipv4
		} else if ipv6, ok := instance.Attributes["AWS_INSTANCE_IPV6"]; ok {
			ip = ipv6
		} else if cname, ok := instance.Attributes["AWS_INSTANCE_CNAME"]; ok {
			ip = cname
		}

		// Extract Dapr port from instance attributes
		if daprPortStr, ok := instance.Attributes["DAPR_PORT"]; ok {
			if parsedPort, err := strconv.Atoi(daprPortStr); err == nil {
				port = parsedPort
			}
		}
	}

	if ip == "" {
		return "", errors.New("no valid IP address found in instance attributes")
	}

	// Use the requested port if specified, otherwise use the instance's Dapr port, otherwise use default
	if requestedPort > 0 {
		port = requestedPort
	} else if port == 0 {
		port = r.defaultDaprPort
	}

	return fmt.Sprintf("%s:%d", ip, port), nil
}

// Close implements io.Closer.
func (r *Resolver) Close() error {
	if r.authProvider != nil {
		return r.authProvider.Close()
	}
	return nil
}

// validateAccess validates access to AWS CloudMap and resolves namespace if needed.
func (r *Resolver) validateAccess(ctx context.Context) error {
	if r.namespaceID != "" {
		return r.validateAccessByID(ctx)
	}
	if r.namespaceName == "" {
		return errors.New("either namespaceName or namespaceId must be provided")
	}
	return r.validateAccessByName(ctx)
}

func (r *Resolver) validateAccessByID(ctx context.Context) error {
	input := &servicediscovery.GetNamespaceInput{
		Id: aws.String(r.namespaceID),
	}
	result, err := r.client.GetNamespace(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to access namespace %s: %w", r.namespaceID, err)
	}
	// Set the namespace name from the response for use in DiscoverInstances
	if result.Namespace != nil && result.Namespace.Name != nil {
		r.namespaceName = *result.Namespace.Name
	}
	return nil
}

func (r *Resolver) validateAccessByName(ctx context.Context) error {
	input := &servicediscovery.ListNamespacesInput{}
	result, err := r.client.ListNamespaces(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to list namespaces: %w", err)
	}
	for _, ns := range result.Namespaces {
		if ns.Name != nil && *ns.Name == r.namespaceName {
			if ns.Id != nil {
				r.namespaceID = *ns.Id
			}
			return nil
		}
	}
	return fmt.Errorf("namespace not found: %s", r.namespaceName)
}