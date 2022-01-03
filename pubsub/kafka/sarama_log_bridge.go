package kafka

import (
	"github.com/dapr/kit/logger"
)

type SaramaLogBridge struct {
	daprLogger logger.Logger
}

func (b SaramaLogBridge) Print(v ...interface{}) {
	b.daprLogger.Debug(v...)
}

func (b SaramaLogBridge) Printf(format string, v ...interface{}) {
	b.daprLogger.Debugf(format, v...)
}

func (b SaramaLogBridge) Println(v ...interface{}) {
	b.daprLogger.Debug(v...)
}
