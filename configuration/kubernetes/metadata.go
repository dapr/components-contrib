/*
Copyright 2026 The Dapr Authors
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
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/dapr/components-contrib/configuration"
	kitmd "github.com/dapr/kit/metadata"
)

// validDNS1123Subdomain matches DNS subdomain names (ConfigMap names): lowercase alphanumeric,
// '-' or '.', must start and end with alphanumeric, max 253 chars.
var validDNS1123Subdomain = regexp.MustCompile(`^[a-z0-9]([a-z0-9\.\-]*[a-z0-9])?$`)

// validDNS1123Label matches DNS labels (namespace names): lowercase alphanumeric or '-',
// must start and end with alphanumeric, max 63 chars.
var validDNS1123Label = regexp.MustCompile(`^[a-z0-9]([a-z0-9\-]*[a-z0-9])?$`)

const maxSubdomainLen = 253

type metadata struct {
	Namespace      string        `mapstructure:"namespace"`
	ConfigMapName  string        `mapstructure:"configMapName"`
	KubeconfigPath string        `mapstructure:"kubeconfigPath"`
	ResyncPeriod   time.Duration `mapstructure:"resyncPeriod"`

	// namespaceExplicit tracks whether the namespace was explicitly set in
	// component metadata, to distinguish from the "default" fallback.
	namespaceExplicit bool
}

func (m *metadata) parse(meta configuration.Metadata) error {
	if err := kitmd.DecodeMetadata(meta.Properties, m); err != nil {
		return err
	}

	if m.ConfigMapName == "" {
		return errors.New("configMapName is required")
	}

	if len(m.ConfigMapName) > maxSubdomainLen || !validDNS1123Subdomain.MatchString(m.ConfigMapName) {
		return fmt.Errorf("configMapName %q is not a valid Kubernetes resource name (must be a DNS subdomain)", m.ConfigMapName)
	}

	m.namespaceExplicit = m.Namespace != ""
	if m.Namespace == "" {
		m.Namespace = "default"
	}

	if !validDNS1123Label.MatchString(m.Namespace) {
		return fmt.Errorf("namespace %q is not a valid Kubernetes namespace name (must be a DNS label)", m.Namespace)
	}

	return nil
}
