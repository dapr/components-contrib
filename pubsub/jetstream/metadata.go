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
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/dapr/components-contrib/pubsub"
	kitmd "github.com/dapr/kit/metadata"
)

type metadata struct {
	NatsURL string `mapstructure:"natsURL"`

	Jwt     string `mapstructure:"jwt"`
	SeedKey string `mapstructure:"seedKey"`
	Token   string `mapstructure:"token"`

	TLSClientCert string `mapstructure:"tls_client_cert"`
	TLSClientKey  string `mapstructure:"tls_client_key"`

	Name                  string             `mapstructure:"name"`
	StreamName            string             `mapstructure:"streamName"`
	DurableName           string             `mapstructure:"durableName"`
	QueueGroupName        string             `mapstructure:"queueGroupName"`
	StartSequence         uint64             `mapstructure:"startSequence"`
	StartTime             *uint64            `mapstructure:"startTime"`
	internalStartTime     time.Time          `mapstructure:"-"`
	FlowControl           bool               `mapstructure:"flowControl"`
	AckWait               time.Duration      `mapstructure:"ackWait"`
	MaxDeliver            int                `mapstructure:"maxDeliver"`
	BackOff               []time.Duration    `mapstructure:"backOff"`
	MaxAckPending         int                `mapstructure:"maxAckPending"`
	Replicas              int                `mapstructure:"replicas"`
	MemoryStorage         bool               `mapstructure:"memoryStorage"`
	RateLimit             uint64             `mapstructure:"rateLimit"`
	Heartbeat             time.Duration      `mapstructure:"heartbeat"`
	DeliverPolicy         string             `mapstructure:"deliverPolicy"`
	internalDeliverPolicy nats.DeliverPolicy `mapstructure:"-"`
	AckPolicy             string             `mapstructure:"ackPolicy"`
	internalAckPolicy     nats.AckPolicy     `mapstructure:"-"`
	Domain                string             `mapstructure:"domain"`
	APIPrefix             string             `mapstructure:"apiPrefix"`

	Concurrency pubsub.ConcurrencyMode `mapstructure:"concurrency"`
}

func parseMetadata(psm pubsub.Metadata) (metadata, error) {
	m := metadata{
		Concurrency: pubsub.Single,
	}

	err := kitmd.DecodeMetadata(psm.Properties, &m)
	if err != nil {
		return metadata{}, err
	}

	if m.NatsURL == "" {
		return metadata{}, errors.New("missing nats URL")
	}

	if m.Jwt != "" && m.SeedKey == "" {
		return metadata{}, errors.New("missing seed key")
	}

	if m.Jwt == "" && m.SeedKey != "" {
		return metadata{}, errors.New("missing jwt")
	}

	if m.TLSClientCert != "" && m.TLSClientKey == "" {
		return metadata{}, errors.New("missing tls client key")
	}

	if m.TLSClientCert == "" && m.TLSClientKey != "" {
		return metadata{}, errors.New("missing tls client cert")
	}

	if m.Name == "" {
		m.Name = "dapr.io - pubsub.jetstream"
	}

	if m.StartTime != nil {
		m.internalStartTime = time.Unix(int64(*m.StartTime), 0) //nolint:gosec
	}

	switch m.DeliverPolicy {
	case "all", "":
		m.internalDeliverPolicy = nats.DeliverAllPolicy
	case "last":
		m.internalDeliverPolicy = nats.DeliverLastPolicy
	case "new":
		m.internalDeliverPolicy = nats.DeliverNewPolicy
	case "sequence":
		m.internalDeliverPolicy = nats.DeliverByStartSequencePolicy
	case "time":
		m.internalDeliverPolicy = nats.DeliverByStartTimePolicy
	default:
		return metadata{}, fmt.Errorf("deliver policy %s is not one of: all, last, new, sequence, time", m.DeliverPolicy)
	}

	switch m.AckPolicy {
	case "explicit":
		m.internalAckPolicy = nats.AckExplicitPolicy
	case "all":
		m.internalAckPolicy = nats.AckAllPolicy
	case "none":
		m.internalAckPolicy = nats.AckNonePolicy
	default:
		m.internalAckPolicy = nats.AckExplicitPolicy
	}

	// Explicit check to prevent overriding the Single default
	// (the previous behavior) if not set.
	// TODO: See https://github.com/dapr/components-contrib/pull/3222#discussion_r1389772053
	if psm.Properties[pubsub.ConcurrencyKey] != "" {
		c, err := pubsub.Concurrency(psm.Properties)
		if err != nil {
			return metadata{}, err
		}
		m.Concurrency = c
	}

	return m, nil
}
