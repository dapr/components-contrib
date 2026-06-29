/*
Copyright 2025 The Dapr Authors
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
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	"github.com/aws/aws-sdk-go-v2/aws"
)

type AwsClients struct {
	config         *sarama.Config
	consumerGroup  *string
	brokers        *[]string
	producerConfig ProducerConfig

	ConsumerGroup sarama.ConsumerGroup
	Producer      sarama.SyncProducer
}
type KafkaOptions struct {
	Config         *sarama.Config
	ConsumerGroup  string
	Brokers        []string
	ProducerConfig ProducerConfig
}

func InitAwsClients(opts KafkaOptions) *AwsClients {
	return &AwsClients{
		config:         opts.Config,
		consumerGroup:  &opts.ConsumerGroup,
		brokers:        &opts.Brokers,
		producerConfig: opts.ProducerConfig,
	}
}

func (c *AwsClients) New(cfg *aws.Config) error {
	const timeout = 10 * time.Second

	tokenProvider := &mskTokenProvider{
		generateTokenTimeout: timeout,
		region:               cfg.Region,
		credentialsProvider:  cfg.Credentials,
	}

	c.config.Net.SASL.Enable = true
	c.config.Net.SASL.Mechanism = sarama.SASLTypeOAuth
	c.config.Net.SASL.TokenProvider = tokenProvider

	_, err := c.config.Net.SASL.TokenProvider.Token()
	if err != nil {
		return fmt.Errorf("error validating iam credentials %v", err)
	}

	consumerGroup, err := sarama.NewConsumerGroup(*c.brokers, *c.consumerGroup, c.config)
	if err != nil {
		return err
	}
	c.ConsumerGroup = consumerGroup

	producer, err := c.getSyncProducer()
	if err != nil {
		return err
	}
	c.Producer = producer

	return nil
}

// Kafka specific
type mskTokenProvider struct {
	generateTokenTimeout time.Duration
	region               string
	credentialsProvider  aws.CredentialsProvider
}

func (m *mskTokenProvider) Token() (*sarama.AccessToken, error) {
	// this function can't use the context passed on Init because that context would be cancelled right after Init
	ctx, cancel := context.WithTimeout(context.Background(), m.generateTokenTimeout)
	defer cancel()

	token, _, err := signer.GenerateAuthTokenFromCredentialsProvider(ctx, m.region, m.credentialsProvider)
	return &sarama.AccessToken{Token: token}, err
}

func (c *AwsClients) getSyncProducer() (sarama.SyncProducer, error) {
	// Apply SyncProducer-specific properties to a COPY of the shared config so
	// the consumer-group's view of the config is not mutated (OSS-1152 fix).
	cfg := *c.config
	cfg.Producer.RequiredAcks = c.producerConfig.RequiredAcks
	cfg.Producer.Retry.Max = c.producerConfig.RetryMax
	cfg.Producer.Return.Successes = true

	if c.producerConfig.MaxMessageBytes > 0 {
		cfg.Producer.MaxMessageBytes = c.producerConfig.MaxMessageBytes
	}

	saramaClient, err := sarama.NewClient(*c.brokers, &cfg)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducerFromClient(saramaClient)
	if err != nil {
		return nil, err
	}

	return producer, nil
}
