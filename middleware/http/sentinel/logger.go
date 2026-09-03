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

package sentinel

import (
	"github.com/dapr/kit/logger"
)

// loggerAdaptor implements sentinel's logging.Logger over the Dapr logger.
//
// Sentinel already logs with a message plus alternating keys and values, which
// maps directly onto the structured logger. The previous adaptor flattened
// everything into one string via logging.AssembleMsg, burying sentinel's own
// timestamp and caller inside the message, and logged warnings and errors at
// info level; both are corrected here.
type loggerAdaptor struct {
	log *logger.Log
}

func newLoggerAdaptor(l logger.Logger) *loggerAdaptor {
	return &loggerAdaptor{log: logger.FromLogger(l)}
}

func (l *loggerAdaptor) Debug(msg string, keysAndValues ...interface{}) {
	l.log.Debug(msg, keysAndValues...)
}

func (l *loggerAdaptor) DebugEnabled() bool {
	return true
}

func (l *loggerAdaptor) Info(msg string, keysAndValues ...interface{}) {
	l.log.Info(msg, keysAndValues...)
}

func (l *loggerAdaptor) InfoEnabled() bool {
	return true
}

func (l *loggerAdaptor) Warn(msg string, keysAndValues ...interface{}) {
	l.log.Warn(msg, keysAndValues...)
}

func (l *loggerAdaptor) WarnEnabled() bool {
	return true
}

func (l *loggerAdaptor) Error(err error, msg string, keysAndValues ...interface{}) {
	args := make([]any, 0, len(keysAndValues)+1)
	args = append(args, keysAndValues...)

	if err != nil {
		args = append(args, logger.Err(err))
	}

	l.log.Error(msg, args...)
}

func (l *loggerAdaptor) ErrorEnabled() bool {
	return true
}
