package test

import (
	"bytes"
	"fmt"

	"github.com/dapr/kit/logger"
)

// compile-time check to ensure testLogger implements logger.testLogger.
var _ logger.Logger = &testLogger{}

func NewLogger() logger.Logger {
	var buf bytes.Buffer
	return &testLogger{buf: &buf}
}

type testLogger struct {
	buf *bytes.Buffer
}

func (l *testLogger) String() string {
	return l.buf.String()
}

func (l *testLogger) EnableJSONOutput(enabled bool) {
	fmt.Fprintf(l.buf, "EnableJSONOutput(%v)\n", enabled)
}

func (l *testLogger) SetAppID(id string) {
	fmt.Fprintf(l.buf, "SetAppID(%v)\n", id)
}

func (l *testLogger) SetOutputLevel(outputLevel logger.LogLevel) {
	fmt.Fprintf(l.buf, "SetOutputLevel(%v)\n", outputLevel)
}

func (l *testLogger) WithLogType(logType string) logger.Logger {
	fmt.Fprintf(l.buf, "WithLogType(%v)\n", logType)
	return l
}

func (l *testLogger) Info(args ...interface{}) {
	fmt.Fprintf(l.buf, "Info(%v)\n", fmt.Sprint(args...))
}

func (l *testLogger) Infof(format string, args ...interface{}) {
	fmt.Fprintf(l.buf, "Info(%v)\n", fmt.Sprintf(format, args...))
}

func (l *testLogger) Debug(args ...interface{}) {
	fmt.Fprintf(l.buf, "Debug(%v)\n", fmt.Sprint(args...))
}

func (l *testLogger) Debugf(format string, args ...interface{}) {
	fmt.Fprintf(l.buf, "Debug(%v)\n", fmt.Sprintf(format, args...))
}

func (l *testLogger) Warn(args ...interface{}) {
	fmt.Fprintf(l.buf, "Warn(%v)\n", fmt.Sprint(args...))
}

func (l *testLogger) Warnf(format string, args ...interface{}) {
	fmt.Fprintf(l.buf, "Warn(%v)\n", fmt.Sprintf(format, args...))
}

func (l *testLogger) Error(args ...interface{}) {
	fmt.Fprintf(l.buf, "Error(%v)\n", fmt.Sprint(args...))
}

func (l *testLogger) Errorf(format string, args ...interface{}) {
	fmt.Fprintf(l.buf, "Error(%v)\n", fmt.Sprintf(format, args...))
}

func (l *testLogger) Fatal(args ...interface{}) {
	fmt.Fprintf(l.buf, "Fatal(%v)\n", fmt.Sprint(args...))
}

func (l *testLogger) Fatalf(format string, args ...interface{}) {
	fmt.Fprintf(l.buf, "Fatal(%v)\n", fmt.Sprintf(format, args...))
}
