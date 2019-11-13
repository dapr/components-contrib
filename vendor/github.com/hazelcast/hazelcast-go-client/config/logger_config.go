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

package config

import (
	"github.com/hazelcast/hazelcast-go-client/core/logger"
)

// LoggerConfig is used for configuring client's logging.
type LoggerConfig struct {
	logger logger.Logger
}

// NewLoggerConfig returns a LoggerConfig with default logger.
func NewLoggerConfig() *LoggerConfig {
	return &LoggerConfig{}
}

// SetLogger sets the loggerConfig's logger as the given one.
// If this method is called, LoggingLevel property wont be used, instead Level should be set
// by user.
func (l *LoggerConfig) SetLogger(logger logger.Logger) {
	l.logger = logger
}

// Logger returns the loggerConfig's logger.
func (l *LoggerConfig) Logger() logger.Logger {
	return l.logger
}
