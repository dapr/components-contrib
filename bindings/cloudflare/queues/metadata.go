//go:generate echo TODO bindings/cloudflare/queues

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

	"github.com/dapr/components-contrib/common/component/cloudflare/workers"

	"github.com/dapr/components-contrib/build-tools/pkg/metadataschema"
	"github.com/dapr/components-contrib/common/component"
)

// implement MetadataBuilder so each component will be properly parsed for the ast to auto-generate metadata manifest.
var _ component.MetadataBuilder = &queuesMetadata{}

// Component metadata struct.
type queuesMetadata struct {
	workers.BaseMetadata `mapstructure:",squash"`
	// Name of the existing Cloudflare Queue
	QueueName string `json:"queueName" mapstructure:"queueName"`
}

var queueNameValidation = regexp.MustCompile(`^([a-zA-Z0-9_\-\.]+)$`)

// Validate the metadata object.
func (q *queuesMetadata) Validate() error {
	// Start by validating the base metadata, then validate the properties specific to this component
	err := q.BaseMetadata.Validate()
	if err != nil {
		return err
	}

	// QueueName
	if q.QueueName == "" {
		return errors.New("property 'queueName' is required")
	}
	if !queueNameValidation.MatchString(q.QueueName) {
		return errors.New("metadata property 'queueName' is invalid")
	}

	return nil
}

// Set the default values here.
// This unifies the setup across all components,
// and makes it easy for us to auto-generate the component metadata default values,
// while also leveraging the default values for types thanks to Go.
func (q *queuesMetadata) Defaults() any {
	return queuesMetadata{}
}

// Note: we do not include any mdignored field.
func (q *queuesMetadata) Examples() any {
	return queuesMetadata{
		QueueName: "mydaprqueue",
		// CfAPIToken:  "secret-key",
		// CfAccountID: "456789abcdef8b5588f3d134f74ac",
		// WorkerURL:   "https://mydaprqueue.mydomain.workers.dev",
	}
}

func (q *queuesMetadata) Binding() metadataschema.Binding {
	return metadataschema.Binding{
		Input:  false,
		Output: true,
		Operations: []metadataschema.BindingOperation{
			{
				Name:        "publish",
				Description: "Publish a new message in the queue.",
			},
			// Note: I added the create bc docs state create is an alias for publish.
			{
				Name:        "create",
				Description: "Publish a new message in the queue.",
			},
		},
	}
}
