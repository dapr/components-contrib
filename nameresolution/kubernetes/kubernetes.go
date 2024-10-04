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

package kubernetes

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"text/template"

	"github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/config"
	"github.com/dapr/kit/logger"
)

const (
	DefaultClusterDomain = "cluster.local"
	ClusterDomainKey     = "clusterDomain"
	TemplateKey          = "template"
)

// Compile-time interface assertions
var (
	_ nameresolution.Resolver      = (*resolver)(nil)
	_ nameresolution.ResolverMulti = (*resolver)(nil)
)

func executeTemplateWithResolveRequest(tmpl *template.Template, req nameresolution.ResolveRequest) (string, error) {
	var addr bytes.Buffer
	if err := tmpl.Execute(&addr, req); err != nil {
		return "", err
	}
	return addr.String(), nil
}

type resolver struct {
	logger        logger.Logger
	clusterDomain string
	tmpl          *template.Template
}

// NewResolver creates Kubernetes name resolver.
func NewResolver(logger logger.Logger) nameresolution.Resolver {
	return &resolver{
		logger:        logger,
		clusterDomain: DefaultClusterDomain,
		tmpl:          nil,
	}
}

// Init initializes Kubernetes name resolver.
func (k *resolver) Init(ctx context.Context, metadata nameresolution.Metadata) error {
	configInterface, err := config.Normalize(metadata.Configuration)
	if err != nil {
		return err
	}

	if cfg, ok := configInterface.(map[string]interface{}); ok {
		clusterDomainAny := cfg[ClusterDomainKey]
		tmplStrAny := cfg[TemplateKey]

		if clusterDomainAny != nil {
			clusterDomain, _ := clusterDomainAny.(string)
			if clusterDomain != "" {
				k.clusterDomain = clusterDomain
			}
		}

		if tmplStrAny != nil {
			tmplStr, _ := tmplStrAny.(string)
			if tmplStr != "" {
				k.tmpl = template.Must(template.New("kubernetes-template").Parse(tmplStr))
				k.logger.Debugf("using custom template %s", tmplStr)
			}
		}
	}

	return nil
}

// ResolveID resolves name to address in Kubernetes.
func (k *resolver) ResolveID(ctx context.Context, req nameresolution.ResolveRequest) (string, error) {
	if k.tmpl != nil {
		return executeTemplateWithResolveRequest(k.tmpl, req)
	}
	// Dapr requires this formatting for Kubernetes services
	return req.ID + "-dapr." + req.Namespace + ".svc." + k.clusterDomain + ":" + strconv.Itoa(req.Port), nil
}

// ResolveIDMulti resolves an app-id to a set of IP addresses in Kubernetes
func (k *resolver) ResolveIDMulti(ctx context.Context, req nameresolution.ResolveRequest) (nameresolution.AddressList, error) {
	// First, get the address from ResolveID, which is usually a DNS name
	addr, err := k.ResolveID(ctx, req)
	if err != nil {
		return nil, err
	}

	// Extract the port if present
	var port string
	idx := strings.LastIndexByte(addr, ':')
	if idx > -1 && (idx+1) < len(addr) {
		port = addr[(idx + 1):]
		addr = addr[:idx]
	}

	// Resolve the DNS name for a list of IPv4 or IPv6
	ips, err := net.LookupIP(addr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve address for '%s': %w", addr, err)
	}

	// Return a list of IPs + port
	res := make(nameresolution.AddressList, len(ips))
	for i, ip := range ips {
		res[i] = net.JoinHostPort(ip.String(), port)
	}
	return res, nil
}

func (k *resolver) Close() error {
	return nil
}
