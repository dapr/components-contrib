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
	"net/url"
	"strings"
	"time"

	// Blank import for the sqlite driver
	_ "modernc.org/sqlite"

	"github.com/dapr/kit/logger"
)

const (
	DefaultTimeout     = 20 * time.Second // Default timeout for database requests, in seconds
	DefaultBusyTimeout = 2 * time.Second
)

// SqliteAuthMetadata contains the auth metadata for a SQLite component.
type SqliteAuthMetadata struct {
	ConnectionString string        `mapstructure:"connectionString" mapstructurealiases:"url"`
	Timeout          time.Duration `mapstructure:"timeout" mapstructurealiases:"timeoutInSeconds"`
	BusyTimeout      time.Duration `mapstructure:"busyTimeout"`
	DisableWAL       bool          `mapstructure:"disableWAL"` // Disable WAL journaling. You should not use WAL if the database is stored on a network filesystem (or data corruption may happen). This is ignored if the database is in-memory.
}

// Reset the object
func (m *SqliteAuthMetadata) Reset() {
	m.ConnectionString = ""
	m.Timeout = DefaultTimeout
	m.BusyTimeout = DefaultBusyTimeout
	m.DisableWAL = false
}

// Validate the auth metadata and returns an error if it's not valid.
func (m *SqliteAuthMetadata) Validate() error {
	// Validate and sanitize input
	if m.ConnectionString == "" {
		return errors.New("missing connection string")
	}
	if m.Timeout < time.Second {
		return errors.New("invalid value for 'timeout': must be greater than 1s")
	}

	// Busy timeout
	// Truncate values to milliseconds. Values <= 0 do not set any timeout
	m.BusyTimeout = m.BusyTimeout.Truncate(time.Millisecond)

	return nil
}

// IsInMemoryDB returns true if the connection string is for an in-memory database.
func (m SqliteAuthMetadata) IsInMemoryDB() bool {
	lc := strings.ToLower(m.ConnectionString)
	return strings.HasPrefix(lc, ":memory:") || strings.HasPrefix(lc, "file::memory:")
}

// GetConnectionStringOpts contains options for GetConnectionString
type GetConnectionStringOpts struct {
	// Enabled foreign keys
	EnableForeignKeys bool
}

// GetConnectionString returns the parsed connection string.
func (m *SqliteAuthMetadata) GetConnectionString(log logger.Logger, opts GetConnectionStringOpts) (string, error) {
	// Check if we're using the in-memory database
	isMemoryDB := m.IsInMemoryDB()

	// Get the "query string" from the connection string if present
	idx := strings.IndexRune(m.ConnectionString, '?')
	var qs url.Values
	if idx > 0 {
		qs, _ = url.ParseQuery(m.ConnectionString[(idx + 1):])
	}
	if len(qs) == 0 {
		qs = make(url.Values, 2)
	}

	// If the database is in-memory, we must ensure that cache=shared is set
	if isMemoryDB {
		qs["cache"] = []string{"shared"}
	}

	// Check if the database is read-only or immutable
	isReadOnly := false
	if len(qs["mode"]) > 0 {
		// Keep the first value only
		qs["mode"] = []string{
			qs["mode"][0],
		}
		if qs["mode"][0] == "ro" {
			isReadOnly = true
		}
	}
	if len(qs["immutable"]) > 0 {
		// Keep the first value only
		qs["immutable"] = []string{
			qs["immutable"][0],
		}
		if qs["immutable"][0] == "1" {
			isReadOnly = true
		}
	}

	// We do not want to override a _txlock if set, but we'll show a warning if it's not "immediate"
	if len(qs["_txlock"]) > 0 {
		// Keep the first value only
		qs["_txlock"] = []string{
			strings.ToLower(qs["_txlock"][0]),
		}
		if qs["_txlock"][0] != "immediate" {
			log.Warn("Database connection is being created with a _txlock different from the recommended value 'immediate'")
		}
	} else {
		qs["_txlock"] = []string{"immediate"}
	}

	// Add pragma values
	if len(qs["_pragma"]) == 0 {
		qs["_pragma"] = make([]string, 0, 3)
	} else {
		for _, p := range qs["_pragma"] {
			p = strings.ToLower(p)
			switch {
			case strings.HasPrefix(p, "busy_timeout"):
				log.Error("Cannot set `_pragma=busy_timeout` option in the connection string; please use the `busyTimeout` metadata property instead")
				return "", errors.New("found forbidden option '_pragma=busy_timeout' in the connection string")
			case strings.HasPrefix(p, "journal_mode"):
				log.Error("Cannot set `_pragma=journal_mode` option in the connection string; please use the `disableWAL` metadata property instead")
				return "", errors.New("found forbidden option '_pragma=journal_mode' in the connection string")
			case strings.HasPrefix(p, "foreign_keys"):
				log.Error("Cannot set `_pragma=foreign_keys` option in the connection string")
				return "", errors.New("found forbidden option '_pragma=foreign_keys' in the connection string")
			}
		}
	}
	if m.BusyTimeout > 0 {
		qs["_pragma"] = append(qs["_pragma"], fmt.Sprintf("busy_timeout(%d)", m.BusyTimeout.Milliseconds()))
	}
	if isMemoryDB {
		// For in-memory databases, set the journal to MEMORY, the only allowed option besides OFF (which would make transactions ineffective)
		qs["_pragma"] = append(qs["_pragma"], "journal_mode(MEMORY)")
	} else if m.DisableWAL || isReadOnly {
		// Set the journaling mode to "DELETE" (the default) if WAL is disabled or if the database is read-only
		qs["_pragma"] = append(qs["_pragma"], "journal_mode(DELETE)")
	} else {
		// Enable WAL
		qs["_pragma"] = append(qs["_pragma"], "journal_mode(WAL)")
	}
	if opts.EnableForeignKeys {
		qs["_pragma"] = append(qs["_pragma"], "foreign_keys(1)")
	}

	// Build the final connection string
	connString := m.ConnectionString
	if idx > 0 {
		connString = connString[:idx]
	}
	connString += "?" + qs.Encode()

	// If the connection string doesn't begin with "file:", add the prefix
	if !strings.HasPrefix(strings.ToLower(m.ConnectionString), "file:") {
		log.Debug("prefix 'file:' added to the connection string")
		connString = "file:" + connString
	}

	return connString, nil
}

// Validates an identifier, such as table or DB name.
func ValidIdentifier(v string) bool {
	if v == "" {
		return false
	}

	// Loop through the string as byte slice as we only care about ASCII characters
	b := []byte(v)
	for i := range b {
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
