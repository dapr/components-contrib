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
