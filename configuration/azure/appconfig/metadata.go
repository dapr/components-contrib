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

package appconfig

import (
	"fmt"
	"time"

	"github.com/dapr/components-contrib/configuration"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

type metadata struct {
	Host                  string        `mapstructure:"host"`
	ConnectionString      string        `mapstructure:"connectionString"`
	MaxRetries            int           `mapstructure:"maxRetries"`
	MaxRetryDelay         time.Duration `mapstructure:"maxRetryDelay"`
	RetryDelay            time.Duration `mapstructure:"retryDelay"`
	SubscribePollInterval time.Duration `mapstructure:"subscribePollInterval"`
	RequestTimeout        time.Duration `mapstructure:"requestTimeout"`
}

func (m *metadata) Parse(log logger.Logger, meta configuration.Metadata) error {
	// Set defaults
	m.MaxRetries = defaultMaxRetries
	m.MaxRetryDelay = defaultMaxRetryDelay
	m.RetryDelay = defaultRetryDelay
	m.SubscribePollInterval = defaultSubscribePollInterval
	m.RequestTimeout = defaultRequestTimeout

	// Decode the metadata
	decodeErr := contribMetadata.DecodeMetadata(meta.Properties, m)
	if decodeErr != nil {
		return decodeErr
	}

	// Validate options
	if m.ConnectionString != "" && m.Host != "" {
		return fmt.Errorf("azure appconfig error: can't set both %s and %s fields in metadata", host, connectionString)
	}

	if m.ConnectionString == "" && m.Host == "" {
		return fmt.Errorf("azure appconfig error: specify %s or %s field in metadata", host, connectionString)
	}

	// In Dapr 1.11, these properties accepted nanoseconds as integers
	// If users pass values larger than 10^6 (before, 1ms; now, 10^6 seconds), they probably set the metadata property for 1.11 in nanoseconds and that's not what they want here
	// TODO: Remove this in Dapr 1.13
	if m.MaxRetryDelay > time.Millisecond*time.Second { //nolint:durationcheck
		log.Warnf("[WARN] Property 'maxRetryDelay' is %v, which is probably incorrect. If you are upgrading from Dapr 1.11, please note that the property is now a Go duration rather than a number of nanoseconds", m.MaxRetryDelay)
	}
	if m.RetryDelay > time.Millisecond*time.Second { //nolint:durationcheck
		log.Warnf("[WARN] Property 'retryDelay' is %v, which is probably incorrect. If you are upgrading from Dapr 1.11, please note that the property is now a Go duration rather than a number of nanoseconds", m.RetryDelay)
	}
	if m.SubscribePollInterval > time.Millisecond*time.Second { //nolint:durationcheck
		log.Warnf("[WARN] Property 'subscribePollInterval' is %v, which is probably incorrect. If you are upgrading from Dapr 1.11, please note that the property is now a Go duration rather than a number of nanoseconds", m.SubscribePollInterval)
	}
	if m.RequestTimeout > time.Millisecond*time.Second { //nolint:durationcheck
		log.Warnf("[WARN] Property 'requestTimeout' is %v, which is probably incorrect. If you are upgrading from Dapr 1.11, please note that the property is now a Go duration rather than a number of nanoseconds", m.RequestTimeout)
	}

	return nil
}
