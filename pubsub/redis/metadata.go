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

package redis

import (
	"time"
)

type metadata struct {
	// The consumer identifier
	consumerID string
	// The interval between checking for pending messages to redelivery (0 disables redelivery)
	redeliverInterval time.Duration
	// The amount time a message must be pending before attempting to redeliver it (0 disables redelivery)
	processingTimeout time.Duration
	// The size of the message queue for processing
	queueDepth uint
	// The number of concurrent workers that are processing messages
	concurrency uint

	// the max len of stream
	maxLenApprox int64
}
