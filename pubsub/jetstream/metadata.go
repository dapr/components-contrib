/*
Copyright 2022 The Dapr Authors
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

package jetstream

import (
	"fmt"
	"strconv"
	"time"

	"github.com/dapr/components-contrib/pubsub"
)

type metadata struct {
	natsURL string
	jwt     string
	seedKey string

	name           string
	durableName    string
	queueGroupName string
	startSequence  uint64
	startTime      time.Time
	deliverAll     bool
	flowControl    bool
}

func parseMetadata(psm pubsub.Metadata) (metadata, error) {
	var m metadata

	if v, ok := psm.Properties["natsURL"]; ok && v != "" {
		m.natsURL = v
	} else {
		return metadata{}, fmt.Errorf("missing nats URL")
	}

	m.jwt = psm.Properties["jwt"]
	m.seedKey = psm.Properties["seedKey"]

	if m.jwt != "" && m.seedKey == "" {
		return metadata{}, fmt.Errorf("missing seed key")
	}

	if m.jwt == "" && m.seedKey != "" {
		return metadata{}, fmt.Errorf("missing jwt")
	}

	if m.name = psm.Properties["name"]; m.name == "" {
		m.name = "dapr.io - pubsub.jetstream"
	}

	m.durableName = psm.Properties["durableName"]
	m.queueGroupName = psm.Properties["queueGroupName"]

	if v, err := strconv.ParseUint(psm.Properties["startSequence"], 10, 64); err == nil {
		m.startSequence = v
	}

	if v, err := strconv.ParseInt(psm.Properties["startTime"], 10, 64); err == nil {
		m.startTime = time.Unix(v, 0)
	}

	if v, err := strconv.ParseBool(psm.Properties["deliverAll"]); err == nil {
		m.deliverAll = v
	}

	if v, err := strconv.ParseBool(psm.Properties["flowControl"]); err == nil {
		m.flowControl = v
	}

	return m, nil
}
