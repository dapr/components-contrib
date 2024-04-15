/*
Copyright 2023 The Dapr Authors
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
	"time"

	"github.com/dapr/components-contrib/common/authentication/aws"
	pgauth "github.com/dapr/components-contrib/common/authentication/postgresql"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	defaultTimeout = 20 * time.Second // Default timeout for network requests
)

type psqlMetadata struct {
	pgauth.PostgresAuthMetadata `mapstructure:",squash"`
	aws.AWSIAM                  `mapstructure:",squash"`
	Timeout                     time.Duration `mapstructure:"timeout" mapstructurealiases:"timeoutInSeconds"`
}

func (m *psqlMetadata) InitWithMetadata(meta map[string]string) error {
	// Reset the object
	m.PostgresAuthMetadata.Reset()
	m.Timeout = defaultTimeout

	err := kitmd.DecodeMetadata(meta, &m)
	if err != nil {
		return err
	}

	opts := pgauth.InitWithMetadataOpts{
		AzureADEnabled: true,
		AWSIAMEnabled:  true,
	}

	// Validate and sanitize input
	// Azure AD & AWS IAM auth is supported for this component
	err = m.PostgresAuthMetadata.InitWithMetadata(meta, opts)
	if err != nil {
		return err
	}

	return nil
}
