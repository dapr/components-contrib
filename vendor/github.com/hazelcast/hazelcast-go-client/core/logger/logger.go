// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logger

import (
	"strings"

	"github.com/hazelcast/hazelcast-go-client/core"
)

const (
	// OffLevel disables logging.
	OffLevel = "off"
	// ErrorLevel level. Logs. Used for errors that should definitely be noted.
	// Commonly used for hooks to send errors to an error tracking service.
	ErrorLevel = "error"
	// WarnLevel level. Non-critical entries that deserve eyes.
	WarnLevel = "warn"
	// InfoLevel level. General operational entries about what's going on inside the
	// application.
	InfoLevel = "info"
	// DebugLevel level. Usually only enabled when debugging. Very verbose logging.
	DebugLevel = "debug"
	// TraceLevel level. Designates finer-grained informational events than the Debug.
	TraceLevel = "trace"

	offLevel = iota * 100
	errorLevel
	warnLevel
	infoLevel
	debugLevel
	traceLevel
)

// nameToLevel is used to get corresponding level for log level strings.
var nameToLevel = map[string]int{
	ErrorLevel: errorLevel,
	WarnLevel:  warnLevel,
	InfoLevel:  infoLevel,
	DebugLevel: debugLevel,
	TraceLevel: traceLevel,
	OffLevel:   offLevel,
}

// Logger is the interface that is used by client for logging.
type Logger interface {
	// Debug logs the given args at debug level.
	Debug(args ...interface{})
	// Trace logs the given args at trace level.
	Trace(args ...interface{})
	// Info logs the given args at info level.
	Info(args ...interface{})
	// Warn logs the given args at warn level.
	Warn(args ...interface{})
	// Error logs the given args at error level.
	Error(args ...interface{})
}

// isValidLogLevel returns true if the given log level is valid.
// The check is done case insensitive.
func isValidLogLevel(logLevel string) bool {
	logLevel = strings.ToLower(logLevel)
	_, found := nameToLevel[logLevel]
	return found
}

// GetLogLevel returns the corresponding log level with the given string if it exists, otherwise returns an error.
func GetLogLevel(logLevel string) (int, error) {
	if !isValidLogLevel(logLevel) {
		return 0, core.NewHazelcastIllegalArgumentError("no log level found for "+logLevel, nil)
	}
	return nameToLevel[logLevel], nil
}
