/*
Copyright 2022 The Dapr Authors
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

package cfqueues

import (
	"errors"
	"regexp"

	"github.com/dapr/components-contrib/internal/component/cloudflare/workers"
)

// Component metadata struct.
// The component can be initialized in two ways:
// - Instantiate the component with a "workerURL": assumes a worker that has been pre-deployed and it's ready to be used; we will not need API tokens
// - Instantiate the component with a "cfAPIToken" and "cfAccountID": Dapr will take care of creating the worker if it doesn't exist (or upgrade it if needed)
type componentMetadata struct {
	workers.BaseMetadata `mapstructure:",squash"`
	QueueName            string `mapstructure:"queueName"`
}

var queueNameValidation = regexp.MustCompile("^([a-zA-Z0-9_\\-\\.]+)$")

// Validate the metadata object.
func (m *componentMetadata) Validate() error {
	// Start by validating the base metadata, then validate the properties specific to this component
	err := m.BaseMetadata.Validate()
	if err != nil {
		return err
	}

	// QueueName
	if m.QueueName == "" {
		return errors.New("property 'queueName' is required")
	}
	if !queueNameValidation.MatchString(m.QueueName) {
		return errors.New("metadata property 'queueName' is invalid")
	}

	return nil
}
