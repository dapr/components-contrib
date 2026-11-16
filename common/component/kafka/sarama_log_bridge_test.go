/*
Copyright 2026 The Dapr Authors
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
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/kit/logger"
)

func TestSaramaLogBridgeLevel(t *testing.T) {
	consumerErr := &sarama.ConsumerError{Topic: "t", Partition: 0, Err: io.ErrUnexpectedEOF}

	tests := map[string]struct {
		log   func(b SaramaLogBridge)
		level string
	}{
		"Println with ConsumerError":         {func(b SaramaLogBridge) { b.Println(consumerErr) }, "error"},
		"Print with ConsumerError":           {func(b SaramaLogBridge) { b.Print(consumerErr) }, "error"},
		"Println with wrapped ConsumerError": {func(b SaramaLogBridge) { b.Println(fmt.Errorf("wrap: %w", consumerErr)) }, "error"},
		"Println with plain text":            {func(b SaramaLogBridge) { b.Println("client/metadata fetching metadata") }, "debug"},
		"Println with other error":           {func(b SaramaLogBridge) { b.Println(errors.New("boom")) }, "debug"},
		"Printf":                             {func(b SaramaLogBridge) { b.Printf("consumer/%s starting", "t") }, "debug"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			l := logger.NewLogger("test-" + name)
			l.EnableJSONOutput(true)
			l.SetOutputLevel(logger.DebugLevel)
			l.SetOutput(&buf)

			tc.log(SaramaLogBridge{daprLogger: l})

			assert.Contains(t, buf.String(), `"level":"`+tc.level+`"`)
		})
	}
}
