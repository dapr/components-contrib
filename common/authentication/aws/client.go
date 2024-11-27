/*
Copyright 2024 The Dapr Authors
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

package aws

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	aws2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/secretsmanager"
	"github.com/aws/aws-sdk-go/service/secretsmanager/secretsmanageriface"
	"github.com/aws/aws-sdk-go/service/ses"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/aws/aws-sdk-go/service/ssm"
	"github.com/aws/aws-sdk-go/service/ssm/ssmiface"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/vmware/vmware-go-kcl/clientlibrary/config"
)

type Clients struct {
	mu sync.RWMutex

	s3             *S3Clients
	Dynamo         *DynamoDBClients
	sns            *SnsClients
	sqs            *SqsClients
	snssqs         *SnsSqsClients
	Secret         *SecretManagerClients
	ParameterStore *ParameterStoreClients
	kinesis        *KinesisClients
	ses            *SesClients
	kafka          *KafkaClients
}

func newClients() *Clients {
	return new(Clients)
}

func (c *Clients) refresh(session *session.Session) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.s3 != nil:
		c.s3.New(session)
	case c.Dynamo != nil:
		c.Dynamo.New(session)
	case c.sns != nil:
		c.sns.New(session)
	case c.sqs != nil:
		c.sqs.New(session)
	case c.snssqs != nil:
		c.snssqs.New(session)
	case c.Secret != nil:
		c.Secret.New(session)
	case c.ParameterStore != nil:
		c.ParameterStore.New(session)
	case c.kinesis != nil:
		c.kinesis.New(session)
	case c.ses != nil:
		c.ses.New(session)
	case c.kafka != nil:
		// Note: we pass in nil for token provider
		// as there are no special fields for x509 auth for it.
		// Only static auth passes it in.
		err := c.kafka.New(session, nil)
		if err != nil {
			return fmt.Errorf("failed to refresh Kafka AWS IAM Config: %w", err)
		}
	}
	return nil
}

type S3Clients struct {
	S3         *s3.S3
	Uploader   *s3manager.Uploader
	Downloader *s3manager.Downloader
}

type DynamoDBClients struct {
	DynamoDB dynamodbiface.DynamoDBAPI
}

type SnsSqsClients struct {
	Sns *sns.SNS
	Sqs *sqs.SQS
	Sts *sts.STS
}

type SnsClients struct {
	Sns *sns.SNS
}

type SqsClients struct {
	Sqs sqsiface.SQSAPI
}

type SecretManagerClients struct {
	Manager secretsmanageriface.SecretsManagerAPI
}

type ParameterStoreClients struct {
	Store ssmiface.SSMAPI
}

type KinesisClients struct {
	Kinesis     kinesisiface.KinesisAPI
	Region      string
	Credentials *credentials.Credentials
}

type SesClients struct {
	Ses *ses.SES
}

type KafkaClients struct {
	config          *sarama.Config
	consumerGroup   *string
	brokers         *[]string
	maxMessageBytes *int

	ConsumerGroup sarama.ConsumerGroup
	Producer      sarama.SyncProducer
}

func (c *S3Clients) New(session *session.Session) {
	refreshedS3 := s3.New(session, session.Config)
	c.S3 = refreshedS3
	c.Uploader = s3manager.NewUploaderWithClient(refreshedS3)
	c.Downloader = s3manager.NewDownloaderWithClient(refreshedS3)
}

func (c *DynamoDBClients) New(session *session.Session) {
	c.DynamoDB = dynamodb.New(session, session.Config)
}

func (c *SnsClients) New(session *session.Session) {
	c.Sns = sns.New(session, session.Config)
}

func (c *SnsSqsClients) New(session *session.Session) {
	c.Sns = sns.New(session, session.Config)
	c.Sqs = sqs.New(session, session.Config)
	c.Sts = sts.New(session, session.Config)
}

func (c *SqsClients) New(session *session.Session) {
	c.Sqs = sqs.New(session, session.Config)
}

func (c *SqsClients) QueueURL(ctx context.Context, queueName string) (*string, error) {
	if c.Sqs != nil {
		resultURL, err := c.Sqs.GetQueueUrlWithContext(ctx, &sqs.GetQueueUrlInput{
			QueueName: aws.String(queueName),
		})
		if resultURL != nil {
			return resultURL.QueueUrl, err
		}
	}
	return nil, errors.New("unable to get queue url due to empty client")
}

func (c *SecretManagerClients) New(session *session.Session) {
	c.Manager = secretsmanager.New(session, session.Config)
}

func (c *ParameterStoreClients) New(session *session.Session) {
	c.Store = ssm.New(session, session.Config)
}

func (c *KinesisClients) New(session *session.Session) {
	c.Kinesis = kinesis.New(session, session.Config)
	c.Region = *session.Config.Region
	c.Credentials = session.Config.Credentials
}

func (c *KinesisClients) Stream(ctx context.Context, streamName string) (*string, error) {
	if c.Kinesis != nil {
		stream, err := c.Kinesis.DescribeStreamWithContext(ctx, &kinesis.DescribeStreamInput{
			StreamName: aws.String(streamName),
		})
		if stream != nil {
			return stream.StreamDescription.StreamARN, err
		}
	}

	return nil, errors.New("unable to get stream arn due to empty client")
}

func (c *KinesisClients) WorkerCfg(ctx context.Context, stream, consumer, mode string) *config.KinesisClientLibConfiguration {
	const sharedMode = "shared"
	if c.Kinesis != nil {
		if mode == sharedMode {
			if c.Credentials != nil {
				kclConfig := config.NewKinesisClientLibConfigWithCredential(consumer,
					stream, c.Region, consumer,
					c.Credentials)
				return kclConfig
			}
		}
	}

	return nil
}

func (c *SesClients) New(session *session.Session) {
	c.Ses = ses.New(session, session.Config)
}

type KafkaOptions struct {
	Config          *sarama.Config
	ConsumerGroup   string
	Brokers         []string
	MaxMessageBytes int
}

func initKafkaClients(opts KafkaOptions) *KafkaClients {
	return &KafkaClients{
		config:          opts.Config,
		consumerGroup:   &opts.ConsumerGroup,
		brokers:         &opts.Brokers,
		maxMessageBytes: &opts.MaxMessageBytes,
	}
}

func (c *KafkaClients) New(session *session.Session, tokenProvider *mskTokenProvider) error {
	const timeout = 10 * time.Second
	creds, err := session.Config.Credentials.Get()
	if err != nil {
		return fmt.Errorf("failed to get credentials from session: %w", err)
	}

	// fill in token provider common fields across x509 and static auth
	if tokenProvider == nil {
		tokenProvider = &mskTokenProvider{}
	}
	tokenProvider.generateTokenTimeout = timeout
	tokenProvider.region = *session.Config.Region
	tokenProvider.accessKey = creds.AccessKeyID
	tokenProvider.secretKey = creds.SecretAccessKey
	tokenProvider.sessionToken = creds.SessionToken

	c.config.Net.SASL.Enable = true
	c.config.Net.SASL.Mechanism = sarama.SASLTypeOAuth
	c.config.Net.SASL.TokenProvider = tokenProvider

	_, err = c.config.Net.SASL.TokenProvider.Token()
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
	accessKey            string
	secretKey            string
	sessionToken         string
	awsIamRoleArn        string
	awsStsSessionName    string
	region               string
}

func (m *mskTokenProvider) Token() (*sarama.AccessToken, error) {
	// this function can't use the context passed on Init because that context would be cancelled right after Init
	ctx, cancel := context.WithTimeout(context.Background(), m.generateTokenTimeout)
	defer cancel()

	switch {
	// we must first check if we are using the assume role auth profile
	case m.awsIamRoleArn != "" && m.awsStsSessionName != "":
		token, _, err := signer.GenerateAuthTokenFromRole(ctx, m.region, m.awsIamRoleArn, m.awsStsSessionName)
		return &sarama.AccessToken{Token: token}, err
	case m.accessKey != "" && m.secretKey != "":
		token, _, err := signer.GenerateAuthTokenFromCredentialsProvider(ctx, m.region, aws2.CredentialsProviderFunc(func(ctx context.Context) (aws2.Credentials, error) {
			return aws2.Credentials{
				AccessKeyID:     m.accessKey,
				SecretAccessKey: m.secretKey,
				SessionToken:    m.sessionToken,
			}, nil
		}))
		return &sarama.AccessToken{Token: token}, err

	default: // load default aws creds
		token, _, err := signer.GenerateAuthToken(ctx, m.region)
		return &sarama.AccessToken{Token: token}, err
	}
}

func (c *KafkaClients) getSyncProducer() (sarama.SyncProducer, error) {
	// Add SyncProducer specific properties to copy of base config
	c.config.Producer.RequiredAcks = sarama.WaitForAll
	c.config.Producer.Retry.Max = 5
	c.config.Producer.Return.Successes = true

	if *c.maxMessageBytes > 0 {
		c.config.Producer.MaxMessageBytes = *c.maxMessageBytes
	}

	saramaClient, err := sarama.NewClient(*c.brokers, c.config)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducerFromClient(saramaClient)
	if err != nil {
		return nil, err
	}

	return producer, nil
}
