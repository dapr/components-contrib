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
	pgauth "github.com/dapr/components-contrib/internal/authentication/postgresql"
	contribMetadata "github.com/dapr/components-contrib/metadata"
)

type psqlMetadata struct {
	pgauth.PostgresAuthMetadata `mapstructure:",squash"`

	// URL is the connection string to connect to the database.
	// Deprecated alias: use connectionString instead.
	URL string `mapstructure:"url"`
}

func (m *psqlMetadata) InitWithMetadata(meta map[string]string) error {
	// Reset the object
	m.PostgresAuthMetadata.Reset()
	m.URL = ""

	err := contribMetadata.DecodeMetadata(meta, &m)
	if err != nil {
		return err
	}

	// Legacy options
	if m.ConnectionString == "" && m.URL != "" {
		m.ConnectionString = m.URL
	}

	// Validate and sanitize input
	// Azure AD auth is supported for this component
	err = m.PostgresAuthMetadata.InitWithMetadata(meta, true)
	if err != nil {
		return err
	}

	return nil
}
