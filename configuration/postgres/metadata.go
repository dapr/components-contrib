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

package postgres

import (
	"errors"
	"fmt"
	"time"

	"github.com/dapr/components-contrib/common/authentication/aws"
	pgauth "github.com/dapr/components-contrib/common/authentication/postgresql"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	defaultTimeout = 20 * time.Second // Default timeout for network requests
)

type metadata struct {
	pgauth.PostgresAuthMetadata `mapstructure:",squash"`
	Timeout                     time.Duration `mapstructure:"timeout" mapstructurealiases:"timeoutInSeconds"`
	ConfigTable                 string        `mapstructure:"table"`
	MaxIdleTimeoutOld           time.Duration `mapstructure:"connMaxIdleTime"` // Deprecated alias for "connectionMaxIdleTime"
	aws.AWSIAM                  `mapstructure:",squash"`
}

func (m *metadata) InitWithMetadata(meta map[string]string) error {
	// Reset the object
	m.PostgresAuthMetadata.Reset()
	m.ConfigTable = ""
	m.MaxIdleTimeoutOld = 0
	m.Timeout = defaultTimeout

	err := kitmd.DecodeMetadata(meta, &m)
	if err != nil {
		return err
	}

	// Legacy options
	if m.ConnectionMaxIdleTime == 0 && m.MaxIdleTimeoutOld > 0 {
		m.ConnectionMaxIdleTime = m.MaxIdleTimeoutOld
	}

	// Validate and sanitize input
	if m.ConfigTable == "" {
		return errors.New("missing postgreSQL configuration table name")
	}
	if len(m.ConfigTable) > maxIdentifierLength {
		return fmt.Errorf("table name is too long - tableName : '%s'. max allowed field length is %d", m.ConfigTable, maxIdentifierLength)
	}
	if !allowedTableNameChars.MatchString(m.ConfigTable) {
		return fmt.Errorf("invalid table name '%s'. non-alphanumerics or upper cased table names are not supported", m.ConfigTable)
	}

	opts := pgauth.InitWithMetadataOpts{
		AzureADEnabled: true,
		AWSIAMEnabled:  true,
	}

	// Azure AD & AWS IAM auth is supported for this component
	err = m.PostgresAuthMetadata.InitWithMetadata(meta, opts)
	if err != nil {
		return err
	}

	return nil
}
