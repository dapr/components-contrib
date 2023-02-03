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
)

const (
	defaultTableName         = "state"
	defaultMetadataTableName = "metadata"
	defaultCleanupInternal   = time.Duration(0) // Disabled by default
	defaultTimeout           = 20 * time.Second // Default timeout for database requests, in seconds
	defaultBusyTimeout       = 2 * time.Second

	errMissingConnectionString = "missing connection string"
	errInvalidIdentifier       = "invalid identifier: %s" // specify identifier type, e.g. "table name"
)

type sqliteMetadataStruct struct {
	ConnectionString   string `json:"connectionString" mapstructure:"connectionString"`
	TableName          string `json:"tableName" mapstructure:"tableName"`
	MetadataTableName  string `json:"metadataTableName" mapstructure:"metadataTableName"`
	TimeoutInSeconds   string `json:"timeoutInSeconds" mapstructure:"timeoutInSeconds"`
	CleanupIntervalStr string `json:"cleanupInterval" mapstructure:"cleanupInterval"` // Cleanup interval as a time.Duration string
	BusyTimeoutStr     string `json:"busyTimeout" mapstructure:"busyTimeout"`         // Busy timeout as a time.Duration string
	DisableWAL         bool   `json:"disableWAL" mapstructure:"disableWAL"`           // Disable WAL journaling. You should not use WAL if the database is stored on a network filesystem (or data corruption may happen). This is ignored if the database is in-memory.

	timeout         time.Duration
	cleanupInterval time.Duration
	busyTimeout     time.Duration
}

func (m *sqliteMetadataStruct) InitWithMetadata(meta state.Metadata) error {
	m.reset()

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
	if m.CleanupIntervalStr != "" {
		parsed, err := time.ParseDuration(m.CleanupIntervalStr)
		if err != nil {
			return fmt.Errorf("invalid value for 'cleanupInterval': %s", m.CleanupIntervalStr)
		}

		// Non-positive value from meta means disable auto cleanup.
		if parsed > 0 {
			m.cleanupInterval = parsed
		}
	}

	// Busy timeout
	// Values must be in milliseconds. Values <= 0 do not set any timeout
	if m.BusyTimeoutStr != "" {
		parsed, err := time.ParseDuration(m.BusyTimeoutStr)
		parsed = parsed.Truncate(time.Millisecond)
		if err != nil || parsed < 0 {
			return fmt.Errorf("invalid value for 'busyTimeout': %s", m.BusyTimeoutStr)
		}
		m.busyTimeout = parsed
	}

	return nil
}

// Reset the object
func (m *sqliteMetadataStruct) reset() {
	m.ConnectionString = ""
	m.TableName = defaultTableName
	m.MetadataTableName = defaultMetadataTableName
	m.TimeoutInSeconds = ""
	m.CleanupIntervalStr = ""
	m.BusyTimeoutStr = ""
	m.DisableWAL = false
	m.timeout = defaultTimeout
	m.cleanupInterval = defaultCleanupInternal
	m.busyTimeout = defaultBusyTimeout
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
