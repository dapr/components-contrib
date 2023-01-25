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

package sqlite

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/ptr"
)

const (
	defaultTableName       = "state"
	defaultCleanupInternal = 3600 // In seconds = 1 hour
	defaultTimeout         = 20   // Default timeout for database requests, in seconds

	errMissingConnectionString = "missing connection string"
	errInvalidIdentifier       = "invalid identifier: %s" // specify identifier type, e.g. "table name"
)

type sqliteMetadataStruct struct {
	ConnectionString         string `json:"connectionString" mapstructure:"connectionString"`
	TableName                string `json:"tableName" mapstructure:"tableName"`
	TimeoutInSeconds         string `json:"timeoutInSeconds" mapstructure:"timeoutInSeconds"`
	CleanupIntervalInSeconds string `json:"cleanupIntervalInSeconds" mapstructure:"cleanupIntervalInSeconds"`

	timeout         time.Duration
	cleanupInterval *time.Duration
}

func (m *sqliteMetadataStruct) InitWithMetadata(meta state.Metadata) error {
	// Reset the object
	m.ConnectionString = ""
	m.TableName = defaultTableName
	m.cleanupInterval = ptr.Of(defaultCleanupInternal * time.Second)
	m.timeout = defaultTimeout * time.Second

	// Decode the metadata
	err := metadata.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return err
	}

	// Validate and sanitize input
	if m.ConnectionString == "" {
		return errors.New(errMissingConnectionString)
	}
	if !validIdentifier(m.TableName) {
		return fmt.Errorf(errInvalidIdentifier, m.TableName)
	}

	// Timeout
	if m.TimeoutInSeconds != "" {
		timeoutInSec, err := strconv.ParseInt(m.TimeoutInSeconds, 10, 0)
		if err != nil {
			return fmt.Errorf("invalid value for 'timeoutInSeconds': %s", m.TimeoutInSeconds)
		}
		if timeoutInSec < 1 {
			return errors.New("invalid value for 'timeoutInSeconds': must be greater than 0")
		}

		m.timeout = time.Duration(timeoutInSec) * time.Second
	}

	// Cleanup interval
	// Nil duration means never clean up expired data
	if m.CleanupIntervalInSeconds != "" {
		cleanupIntervalInSec, err := strconv.ParseInt(m.CleanupIntervalInSeconds, 10, 0)
		if err != nil {
			return fmt.Errorf("invalid value for 'cleanupIntervalInSeconds': %s", m.CleanupIntervalInSeconds)
		}

		// Non-positive value from meta means disable auto cleanup.
		if cleanupIntervalInSec > 0 {
			m.cleanupInterval = ptr.Of(time.Duration(cleanupIntervalInSec) * time.Second)
		} else {
			m.cleanupInterval = nil
		}
	}

	return nil
}

// Validates an identifier, such as table or DB name.
func validIdentifier(v string) bool {
	if v == "" {
		return false
	}

	// Loop through the string as byte slice as we only care about ASCII characters
	b := []byte(v)
	for i := 0; i < len(b); i++ {
		if (b[i] >= '0' && b[i] <= '9') ||
			(b[i] >= 'a' && b[i] <= 'z') ||
			(b[i] >= 'A' && b[i] <= 'Z') ||
			b[i] == '_' {
			continue
		}
		return false
	}
	return true
}
