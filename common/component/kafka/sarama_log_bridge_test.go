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

package kafka

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dapr/kit/logger"
)

func newTestSaramaLogBridge(buf *bytes.Buffer) SaramaLogBridge {
	l := logger.NewLogger("sarama_log_bridge_test")
	l.SetOutputLevel(logger.DebugLevel)
	l.SetOutput(buf)
	return SaramaLogBridge{daprLogger: l}
}

func TestSaramaLogBridgeErrorMessagesLoggedAsError(t *testing.T) {
	var buf bytes.Buffer
	b := newTestSaramaLogBridge(&buf)

	b.Printf("kafka: %s", "error while consuming topic/0: read tcp: i/o timeout")

	require.Contains(t, buf.String(), "level=error")
}

func TestSaramaLogBridgeNonErrorMessagesLoggedAsDebug(t *testing.T) {
	var buf bytes.Buffer
	b := newTestSaramaLogBridge(&buf)

	b.Print("kafka: connected to broker")

	require.Contains(t, buf.String(), "level=debug")
}

func TestSaramaLogBridgePrintlnErrorMessage(t *testing.T) {
	var buf bytes.Buffer
	b := newTestSaramaLogBridge(&buf)

	b.Println("kafka: error while consuming: client has run out of available brokers to talk to")

	require.Contains(t, buf.String(), "level=error")
}
