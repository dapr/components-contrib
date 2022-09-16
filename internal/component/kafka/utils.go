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
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
)

// asBase64String implements the `fmt.Stringer` interface in order to print
// `[]byte` as a base 64 encoded string.
// It is used above to log the message key. The call to `EncodeToString`
// only occurs for logs that are written based on the logging level.
type asBase64String []byte

func (s asBase64String) String() string {
	return base64.StdEncoding.EncodeToString(s)
}

func parseInitialOffset(value string) (initialOffset int64, err error) {
	initialOffset = sarama.OffsetNewest // Default
	if strings.EqualFold(value, "oldest") {
		initialOffset = sarama.OffsetOldest
	} else if strings.EqualFold(value, "newest") {
		initialOffset = sarama.OffsetNewest
	} else if value != "" {
		return 0, fmt.Errorf("kafka error: invalid initialOffset: %s", value)
	}

	return initialOffset, err
}

// isValidPEM validates the provided input has PEM formatted block.
func isValidPEM(val string) bool {
	block, _ := pem.Decode([]byte(val))

	return block != nil
}

// Map of topics and their handlers
type TopicHandlers map[string]EventHandler

// TopicBulkHandlerConfig is the map of topics and sruct containing bulk handler and their config.
type TopicBulkHandlerConfig map[string]BulkSubscriptionHandlerConfig

// TopicList returns the list of topics
func (th TopicHandlers) TopicList() []string {
	topics := make([]string, len(th))
	i := 0
	for topic := range th {
		topics[i] = topic
		i++
	}
	return topics
}

func (tbh TopicBulkHandlerConfig) TopicList() []string {
	topics := make([]string, len(tbh))
	i := 0
	for topic := range tbh {
		topics[i] = topic
		i++
	}
	return topics
}

// GetIntFromMetadata returns an int value from metadata OR default value if key not found or if its
// value not convertible to int.
func GetIntFromMetadata(metadata map[string]string, key string, defaultValue int) int {
	if val, ok := metadata[key]; ok {
		if intVal, err := strconv.Atoi(val); err == nil {
			return intVal
		}
	}
	return defaultValue
}
