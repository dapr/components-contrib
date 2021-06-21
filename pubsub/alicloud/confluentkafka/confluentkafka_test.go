// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package confluentkafka

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

func TestConfluentKafka(t *testing.T) {
	l := logger.NewLogger("confluentKafkaTest")

	kafka := NewConfluentKafka(l)

	props := pubsub.Metadata{
		Properties: map[string]string{
			"configJson": fmt.Sprintf(
				`{
					"group.id": "dapr_test_group",
					"bootstrap.servers" : "%v",
					"security.protocol" : "sasl_ssl",
					"sasl.mechanism" : "PLAIN",
					"sasl.username" : "%v",
					"sasl.password" : "%v",
					"ssl.ca.location": "%v",
					"api.version.request": true,
					"message.max.bytes": 1000000,
					"linger.ms": 10,
					"retries": 30,
					"retry.backoff.ms": 1000,
					"acks": "1"
				}`,
				os.Getenv("KAFKA_ENDPOINTS"),
				os.Getenv("KAFKA_SASL_USERNAME"),
				os.Getenv("KAFKA_SASL_PASSWORD"),
				os.Getenv("KAFKA_CA_LOCATION"),
			),
		},
	}

	err := kafka.Init(props)
	if err != nil {
		t.Error(err)
	}

	topic := "dapr_test"
	value := "dapr_test_value"
	key := "dapr_test_key"
	count := 0

	err = kafka.Subscribe(
		pubsub.SubscribeRequest{Topic: topic},
		func(ctx context.Context, msg *pubsub.NewMessage) error {
			fmt.Printf("%v, metadata: %+v\n", string(msg.Data), msg.Metadata)
			assert.Equal(t, value, string(msg.Data))
			count++
			return nil
		},
	)
	if err != nil {
		t.Error(err)
	}

	req := pubsub.PublishRequest{
		Data:  []byte(value),
		Topic: topic,
		Metadata: map[string]string{
			"key": key,
			"headers": `{
				"header1" : "1header",
				"header2" : "2header"
			}`,
		},
	}

	err = kafka.Publish(&req)
	if err != nil {
		t.Error(err)
	}

	err = kafka.Close()
	if err != nil {
		t.Error(err)
	}

	if count == 0 {
		t.Errorf("didn't receive any message")
	}
}
