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

// RFC 1123 DNS label: lowercase alphanumeric or '-', must start and end with alphanumeric.
var validKubernetesName = regexp.MustCompile(`^[a-z0-9]([a-z0-9\-]*[a-z0-9])?$`)

type metadata struct {
	Namespace      string        `mapstructure:"namespace"`
	ConfigMapName  string        `mapstructure:"configMapName"`
	KubeconfigPath string        `mapstructure:"kubeconfigPath"`
	ResyncPeriod   time.Duration `mapstructure:"resyncPeriod"`
}

func (m *metadata) parse(meta configuration.Metadata) error {
	if err := kitmd.DecodeMetadata(meta.Properties, m); err != nil {
		return err
	}

	if m.ConfigMapName == "" {
		return errors.New("configMapName is required")
	}

	if !validKubernetesName.MatchString(m.ConfigMapName) {
		return fmt.Errorf("configMapName %q is not a valid Kubernetes resource name", m.ConfigMapName)
	}

	if m.Namespace == "" {
		m.Namespace = "default"
	}

	if !validKubernetesName.MatchString(m.Namespace) {
		return fmt.Errorf("namespace %q is not a valid Kubernetes namespace name", m.Namespace)
	}

	return nil
}
