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
	"time"

	"github.com/dapr/components-contrib/common/component/cloudflare/workers"
)

// Defaults and limits for the input binding, matching the Cloudflare Queues pull consumer API.
const (
	defaultBatchSize         = 5
	maxBatchSize             = 100
	defaultVisibilityTimeout = 30 * time.Second
	maxVisibilityTimeout     = 12 * time.Hour
	defaultPollingInterval   = 10 * time.Second
)

// Component metadata struct.
type componentMetadata struct {
	workers.BaseMetadata `mapstructure:",squash"`
	QueueName            string        `mapstructure:"queueName"`
	QueueID              string        `mapstructure:"queueID"`
	BatchSize            int           `mapstructure:"batchSize"`
	VisibilityTimeout    time.Duration `mapstructure:"visibilityTimeout"`
	PollingInterval      time.Duration `mapstructure:"pollingInterval"`
}

var (
	queueNameValidation = regexp.MustCompile(`^([a-zA-Z0-9_\-\.]+)$`)
	queueIDValidation   = regexp.MustCompile(`^([a-zA-Z0-9]+)$`)
)

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

	// QueueID is optional: when empty, the input binding looks the queue up by name
	if m.QueueID != "" && !queueIDValidation.MatchString(m.QueueID) {
		return errors.New("metadata property 'queueID' is invalid")
	}

	// Input binding options
	if m.BatchSize == 0 {
		m.BatchSize = defaultBatchSize
	}
	if m.BatchSize < 1 || m.BatchSize > maxBatchSize {
		return errors.New("metadata property 'batchSize' must be between 1 and 100")
	}
	if m.VisibilityTimeout == 0 {
		m.VisibilityTimeout = defaultVisibilityTimeout
	}
	if m.VisibilityTimeout < time.Second || m.VisibilityTimeout > maxVisibilityTimeout {
		return errors.New("metadata property 'visibilityTimeout' must be between 1s and 12h")
	}
	if m.PollingInterval == 0 {
		m.PollingInterval = defaultPollingInterval
	}
	if m.PollingInterval < time.Second {
		return errors.New("metadata property 'pollingInterval' must be at least 1s")
	}

	return nil
}
