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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func TestInitRejectsConsumerTransactions(t *testing.T) {
	// Consumer transactions need the delivery's token echoed back through the
	// same component instance; input and output bindings are separate
	// instances, so the flag must fail fast instead of degrading silently.
	b := NewKafka(logger.NewLogger("kafka_binding_test"))

	err := b.Init(t.Context(), bindings.Metadata{Base: contribMetadata.Base{Properties: map[string]string{
		"brokers":                     "localhost:9092",
		"authType":                    "none",
		"consumerTransactionsEnabled": "true",
	}}})

	require.ErrorContains(t, err, "not supported for the Kafka binding")
}
