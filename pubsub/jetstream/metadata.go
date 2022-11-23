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

package jetstream

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dapr/components-contrib/pubsub"
)

type metadata struct {
	natsURL string

	jwt     string
	seedKey string
	token   string

	tlsClientCert string
	tlsClientKey  string

	name           string
	streamName     string
	durableName    string
	queueGroupName string
	startSequence  uint64
	startTime      time.Time
	deliverAll     bool
	flowControl    bool
	ackWait        time.Duration
	maxDeliver     int
	backOff        []time.Duration
	maxAckPending  int
	replicas       int
	memoryStorage  bool
	rateLimit      uint64
	hearbeat       time.Duration
}

func parseMetadata(psm pubsub.Metadata) (metadata, error) {
	var m metadata

	if v, ok := psm.Properties["natsURL"]; ok && v != "" {
		m.natsURL = v
	} else {
		return metadata{}, fmt.Errorf("missing nats URL")
	}

	m.token = psm.Properties["token"]
	m.jwt = psm.Properties["jwt"]
	m.seedKey = psm.Properties["seedKey"]

	if m.jwt != "" && m.seedKey == "" {
		return metadata{}, fmt.Errorf("missing seed key")
	}

	if m.jwt == "" && m.seedKey != "" {
		return metadata{}, fmt.Errorf("missing jwt")
	}

	m.tlsClientCert = psm.Properties["tls_client_cert"]
	m.tlsClientKey = psm.Properties["tls_client_key"]

	if m.tlsClientCert != "" && m.tlsClientKey == "" {
		return metadata{}, fmt.Errorf("missing tls client key")
	}

	if m.tlsClientCert == "" && m.tlsClientKey != "" {
		return metadata{}, fmt.Errorf("missing tls client cert")
	}

	if m.name = psm.Properties["name"]; m.name == "" {
		m.name = "dapr.io - pubsub.jetstream"
	}

	m.durableName = psm.Properties["durableName"]
	if val, ok := psm.Properties["queueGroupName"]; ok && val != "" {
		m.queueGroupName = val
	} else {
		m.queueGroupName = psm.Properties[pubsub.RuntimeConsumerIDKey]
	}

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
	if v, err := time.ParseDuration(psm.Properties["ackWait"]); err == nil {
		m.ackWait = v
	}

	if v, err := strconv.Atoi(psm.Properties["maxDeliver"]); err == nil {
		m.maxDeliver = v
	}

	backOffSlice := strings.Split(psm.Properties["backOff"], ",")
	var backOff []time.Duration

	for _, item := range backOffSlice {
		trimmed := strings.TrimSpace(item)
		if duration, err := time.ParseDuration(trimmed); err == nil {
			backOff = append(backOff, duration)
		}
	}
	m.backOff = backOff

	if v, err := strconv.Atoi(psm.Properties["maxAckPending"]); err == nil {
		m.maxAckPending = v
	}
	if v, err := strconv.Atoi(psm.Properties["replicas"]); err == nil {
		m.replicas = v
	}
	if v, err := strconv.ParseBool(psm.Properties["memoryStorage"]); err == nil {
		m.memoryStorage = v
	}
	if v, err := strconv.ParseUint(psm.Properties["rateLimit"], 10, 64); err == nil {
		m.rateLimit = v
	}

	if v, err := time.ParseDuration(psm.Properties["hearbeat"]); err == nil {
		m.hearbeat = v
	}

	m.streamName = psm.Properties["streamName"]

	return m, nil
}
