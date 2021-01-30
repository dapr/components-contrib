// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/components-contrib/tests/conformance/utils"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/sets"
)

const (
	defaultPubsubName      = "pubusub"
	defaultTopicName       = "testTopic"
	defaultMessageCount    = 10
	defaultMaxReadDuration = 10 * time.Millisecond
)

type TestConfig struct {
	utils.CommonConfig
	pubsubName        string
	testTopicName     string
	publishMetadata   map[string]string
	subscribeMetadata map[string]string
	messageCount      int
	maxReadDuration   time.Duration
}

func NewTestConfig(componentName string, allOperations bool, operations []string, config map[string]string) TestConfig {
	tc := TestConfig{
		CommonConfig: utils.CommonConfig{
			ComponentType: "pubsub",
			ComponentName: componentName,
			AllOperations: allOperations,
			Operations:    sets.NewString(operations...)},
		pubsubName:        defaultPubsubName,
		testTopicName:     defaultTopicName,
		messageCount:      defaultMessageCount,
		maxReadDuration:   defaultMaxReadDuration,
		publishMetadata:   map[string]string{},
		subscribeMetadata: map[string]string{},
	}
	for k, v := range config {
		if k == "pubsubName" {
			tc.pubsubName = v
		}
		if k == "testTopicName" {
			tc.testTopicName = v
		}
		if k == "messageCount" {
			val, err := strconv.Atoi(v)
			if err == nil {
				tc.messageCount = val
			}
		}
		if k == "maxReadDuration" {
			val, err := strconv.Atoi(v)
			if err == nil {
				tc.maxReadDuration = time.Duration(val) * time.Millisecond
			}
		}
		if strings.HasPrefix(k, "publish_") {
			tc.publishMetadata[strings.Replace(k, "publish_", "", 1)] = v
		}
		if strings.HasPrefix(k, "subscribe_") {
			tc.subscribeMetadata[strings.Replace(k, "subscribe_", "", 1)] = v
		}
	}

	return tc
}

func ConformanceTests(t *testing.T, props map[string]string, ps pubsub.PubSub, config TestConfig) {
	// Properly close pubsub
	defer ps.Close()

	actualReadCount := 0

	// Init
	t.Run("init", func(t *testing.T) {
		err := ps.Init(pubsub.Metadata{
			Properties: props,
		})
		assert.NoError(t, err, "expected no error on setting up pubsub")
	})

	// Subscribe
	if config.HasOperation("subscribe") {
		t.Run("subscribe", func(t *testing.T) {
			err := ps.Subscribe(pubsub.SubscribeRequest{
				Topic:    config.testTopicName,
				Metadata: config.subscribeMetadata,
			}, func(_ *pubsub.NewMessage) error {
				actualReadCount++

				return nil
			})
			assert.NoError(t, err, "expected no error on subscribe")
		})
	}

	// Publish
	if config.HasOperation("publish") {
		t.Run("publish", func(t *testing.T) {
			for k := 0; k < config.messageCount; k++ {
				data := []byte("message-" + strconv.Itoa(k))
				err := ps.Publish(&pubsub.PublishRequest{
					Data:       data,
					PubsubName: config.pubsubName,
					Topic:      config.testTopicName,
					Metadata:   config.publishMetadata,
				})
				assert.NoError(t, err, "expected no error on publishing data %s", data)
			}
		})
	}

	// Verify read
	if config.HasOperation("subscribe") {
		t.Run("verify read", func(t *testing.T) {
			t.Logf("waiting for %v to complete read", config.maxReadDuration)
			time.Sleep(config.maxReadDuration)
			assert.LessOrEqual(t, config.messageCount, actualReadCount, "expected to read %v messages", config.messageCount)
		})
	}
}
