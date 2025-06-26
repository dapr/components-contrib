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

package cloudmap

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/servicediscovery"
	"github.com/aws/aws-sdk-go/service/servicediscovery/servicediscoveryiface"

	awsAuth "github.com/dapr/components-contrib/common/authentication/aws"
	"github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

// Resolver is the AWS CloudMap name resolver.
type Resolver struct {
	authProvider    awsAuth.Provider
	client          servicediscoveryiface.ServiceDiscoveryAPI
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

	if err := meta.Validate(); err != nil {
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

	// Create AWS session
	sess, err := session.NewSession(cfg)
	if err != nil {
		return fmt.Errorf("failed to create AWS session: %w", err)
	}

	// Create CloudMap client if not already set (for testing)
	if r.client == nil {
		r.client = servicediscovery.New(sess)
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
		return "", fmt.Errorf("no healthy instances found for service %s", req.ID)
	}

	// Pick a random address for load balancing
	return addresses[rand.Intn(len(addresses))], nil
}

// ResolveIDMulti resolves a service ID to multiple addresses using AWS CloudMap.
func (r *Resolver) ResolveIDMulti(ctx context.Context, req nameresolution.ResolveRequest) (nameresolution.AddressList, error) {
	return r.resolveIDMulti(ctx, req)
}

func (r *Resolver) resolveIDMulti(ctx context.Context, req nameresolution.ResolveRequest) ([]string, error) {
	// Prepare discovery input
	input := &servicediscovery.DiscoverInstancesInput{
		NamespaceName: aws.String(r.namespaceName),
		ServiceName:   aws.String(req.ID),
		HealthStatus:  aws.String(servicediscovery.HealthStatusHealthy),
	}

	r.logger.Debugf("Discovering instances in CloudMap: namespace=%s service=%s", *input.NamespaceName, *input.ServiceName)

	// Call CloudMap API
	result, err := r.client.DiscoverInstancesWithContext(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to discover CloudMap instances: %w", err)
	}

	r.logger.Debugf("Found %d instances for service %s", len(result.Instances), req.ID)

	// Extract addresses from instances
	addresses := make([]string, 0, len(result.Instances))
	for _, instance := range result.Instances {
		if instance.InstanceId == nil || instance.Attributes == nil {
			r.logger.Warnf("Skipping instance with nil ID or attributes")
			continue
		}

		// Get IP/hostname from attributes
		var addr string
		if ipv4, ok := instance.Attributes["AWS_INSTANCE_IPV4"]; ok && ipv4 != nil {
			addr = *ipv4
		} else if ipv6, ok := instance.Attributes["AWS_INSTANCE_IPV6"]; ok && ipv6 != nil {
			addr = *ipv6
		} else if cname, ok := instance.Attributes["AWS_INSTANCE_CNAME"]; ok && cname != nil {
			addr = *cname
		} else {
			r.logger.Warnf("Instance %s has no valid address attributes", *instance.InstanceId)
			continue
		}

		// Get port from DAPR_PORT attribute or use default
		port := r.defaultDaprPort
		if daprPort, ok := instance.Attributes["DAPR_PORT"]; ok && daprPort != nil {
			if p, err := strconv.Atoi(*daprPort); err == nil {
				port = p
			} else {
				r.logger.Warnf("Invalid DAPR_PORT value for instance %s: %s, using default port %d", *instance.InstanceId, *daprPort, r.defaultDaprPort)
			}
		}

		addr = fmt.Sprintf("%s:%d", addr, port)
		addresses = append(addresses, addr)
	}

	if len(addresses) == 0 {
		r.logger.Warnf("No healthy instances found for service %s", req.ID)
	} else {
		r.logger.Debugf("Resolved addresses for service %s: %v", req.ID, addresses)
	}

	return addresses, nil
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
	// If we have namespace ID, validate it and get the name
	if r.namespaceID != "" {
		input := &servicediscovery.GetNamespaceInput{
			Id: aws.String(r.namespaceID),
		}
		result, err := r.client.GetNamespaceWithContext(ctx, input)
		if err != nil {
			return fmt.Errorf("failed to get namespace with ID %s: %w", r.namespaceID, err)
		}
		if result.Namespace != nil && result.Namespace.Name != nil {
			r.namespaceName = *result.Namespace.Name
			return nil
		}
		return fmt.Errorf("namespace ID %s exists but has no name", r.namespaceID)
	}

	// Otherwise, look up namespace by name
	if r.namespaceName == "" {
		return fmt.Errorf("either namespaceName or namespaceId must be provided")
	}

	input := &servicediscovery.ListNamespacesInput{}
	result, err := r.client.ListNamespacesWithContext(ctx, input)
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
