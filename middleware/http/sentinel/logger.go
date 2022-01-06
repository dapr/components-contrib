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
	"github.com/alibaba/sentinel-golang/logging"

	"github.com/dapr/kit/logger"
)

type loggerAdaptor struct {
	logger logger.Logger
}

func (l *loggerAdaptor) Debug(msg string, keysAndValues ...interface{}) {
	s := logging.AssembleMsg(logging.GlobalCallerDepth, "DEBUG", msg, nil, keysAndValues...)
	l.logger.Debug(s)
}

func (l *loggerAdaptor) DebugEnabled() bool {
	return true
}

func (l *loggerAdaptor) Info(msg string, keysAndValues ...interface{}) {
	s := logging.AssembleMsg(logging.GlobalCallerDepth, "INFO", msg, nil, keysAndValues...)
	l.logger.Info(s)
}

func (l *loggerAdaptor) InfoEnabled() bool {
	return true
}

func (l *loggerAdaptor) Warn(msg string, keysAndValues ...interface{}) {
	s := logging.AssembleMsg(logging.GlobalCallerDepth, "WARNING", msg, nil, keysAndValues...)
	l.logger.Info(s)
}

func (l *loggerAdaptor) WarnEnabled() bool {
	return true
}

func (l *loggerAdaptor) Error(err error, msg string, keysAndValues ...interface{}) {
	s := logging.AssembleMsg(logging.GlobalCallerDepth, "ERROR", msg, nil, keysAndValues...)
	l.logger.Info(s)
}

func (l *loggerAdaptor) ErrorEnabled() bool {
	return true
}
