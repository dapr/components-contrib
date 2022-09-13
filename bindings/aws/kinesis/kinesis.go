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

package kinesis

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"github.com/vmware/vmware-go-kcl/clientlibrary/config"
	"github.com/vmware/vmware-go-kcl/clientlibrary/interfaces"
	"github.com/vmware/vmware-go-kcl/clientlibrary/worker"

	"github.com/dapr/components-contrib/bindings"
	awsAuth "github.com/dapr/components-contrib/internal/authentication/aws"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// AWSKinesis allows receiving and sending data to/from AWS Kinesis stream.
type AWSKinesis struct {
	client   *kinesis.Kinesis
	metadata *kinesisMetadata

	worker       *worker.Worker
	workerConfig *config.KinesisClientLibConfiguration

	streamARN   *string
	consumerARN *string
	logger      logger.Logger
}

type kinesisMetadata struct {
	StreamName          string `json:"streamName"`
	ConsumerName        string `json:"consumerName"`
	Region              string `json:"region"`
	Endpoint            string `json:"endpoint"`
	AccessKey           string `json:"accessKey"`
	SecretKey           string `json:"secretKey"`
	SessionToken        string `json:"sessionToken"`
	KinesisConsumerMode string `json:"mode" mapstructure:"mode"`
}

const (
	// ExtendedFanout - dedicated throughput through data stream api.
	ExtendedFanout = "extended"

	// SharedThroughput - shared throughput using checkpoint and monitoring.
	SharedThroughput = "shared"

	partitionKeyName = "partitionKey"
)

// recordProcessorFactory.
type recordProcessorFactory struct {
	ctx     context.Context
	logger  logger.Logger
	handler bindings.Handler
}

type recordProcessor struct {
	ctx     context.Context
	logger  logger.Logger
	handler bindings.Handler
}

// NewAWSKinesis returns a new AWS Kinesis instance.
func NewAWSKinesis(logger logger.Logger) bindings.InputOutputBinding {
	return &AWSKinesis{logger: logger}
}

// Init does metadata parsing and connection creation.
func (a *AWSKinesis) Init(metadata bindings.Metadata) error {
	m, err := a.parseMetadata(metadata)
	if err != nil {
		return err
	}

	if m.KinesisConsumerMode == "" {
		m.KinesisConsumerMode = SharedThroughput
	}

	if m.KinesisConsumerMode != SharedThroughput && m.KinesisConsumerMode != ExtendedFanout {
		return fmt.Errorf("%s invalid \"mode\" field %s", "aws.kinesis", m.KinesisConsumerMode)
	}

	client, err := a.getClient(m)
	if err != nil {
		return err
	}

	streamName := aws.String(m.StreamName)
	stream, err := client.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: streamName,
	})
	if err != nil {
		return err
	}

	if m.KinesisConsumerMode == SharedThroughput {
		kclConfig := config.NewKinesisClientLibConfigWithCredential(m.ConsumerName,
			m.StreamName, m.Region, m.ConsumerName,
			credentials.NewStaticCredentials(m.AccessKey, m.SecretKey, ""))
		a.workerConfig = kclConfig
	}

	a.streamARN = stream.StreamDescription.StreamARN
	a.metadata = m
	a.client = client

	return nil
}

func (a *AWSKinesis) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (a *AWSKinesis) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	partitionKey := req.Metadata[partitionKeyName]
	if partitionKey == "" {
		partitionKey = uuid.New().String()
	}
	_, err := a.client.PutRecordWithContext(ctx, &kinesis.PutRecordInput{
		StreamName:   &a.metadata.StreamName,
		Data:         req.Data,
		PartitionKey: &partitionKey,
	})

	return nil, err
}

func (a *AWSKinesis) Read(ctx context.Context, handler bindings.Handler) (err error) {
	if a.metadata.KinesisConsumerMode == SharedThroughput {
		a.worker = worker.NewWorker(a.recordProcessorFactory(ctx, handler), a.workerConfig)
		err = a.worker.Start()
		if err != nil {
			return err
		}
	} else if a.metadata.KinesisConsumerMode == ExtendedFanout {
		var stream *kinesis.DescribeStreamOutput
		stream, err = a.client.DescribeStream(&kinesis.DescribeStreamInput{StreamName: &a.metadata.StreamName})
		if err != nil {
			return err
		}
		err = a.Subscribe(ctx, *stream.StreamDescription, handler)
		if err != nil {
			return err
		}
	}

	// Wait for context cancelation then stop
	go func() {
		<-ctx.Done()
		if a.metadata.KinesisConsumerMode == SharedThroughput {
			a.worker.Shutdown()
		} else if a.metadata.KinesisConsumerMode == ExtendedFanout {
			a.deregisterConsumer(a.streamARN, a.consumerARN)
		}
	}()

	return nil
}

// Subscribe to all shards.
func (a *AWSKinesis) Subscribe(ctx context.Context, streamDesc kinesis.StreamDescription, handler bindings.Handler) error {
	consumerARN, err := a.ensureConsumer(ctx, streamDesc.StreamARN)
	if err != nil {
		a.logger.Error(err)
		return err
	}

	a.consumerARN = consumerARN

	for i, shard := range streamDesc.Shards {
		go func(idx int, s *kinesis.Shard) error {
			// Reconnection backoff
			bo := backoff.NewExponentialBackOff()
			bo.InitialInterval = 2 * time.Second

			// Repeat until context is canceled
			for ctx.Err() == nil {
				sub, err := a.client.SubscribeToShardWithContext(ctx, &kinesis.SubscribeToShardInput{
					ConsumerARN:      consumerARN,
					ShardId:          s.ShardId,
					StartingPosition: &kinesis.StartingPosition{Type: aws.String(kinesis.ShardIteratorTypeLatest)},
				})
				if err != nil {
					wait := bo.NextBackOff()
					a.logger.Errorf("Error while reading from shard %v: %v. Attempting to reconnect in %s...", s.ShardId, err, wait)
					time.Sleep(wait)
					continue
				}

				// Reset the backoff on connection success
				bo.Reset()

				// Process events
				for event := range sub.EventStream.Events() {
					switch e := event.(type) {
					case *kinesis.SubscribeToShardEvent:
						for _, rec := range e.Records {
							handler(ctx, &bindings.ReadResponse{
								Data: rec.Data,
							})
						}
					}
				}
			}
			return nil
		}(i, shard)
	}

	return nil
}

func (a *AWSKinesis) ensureConsumer(parentCtx context.Context, streamARN *string) (*string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	consumer, err := a.client.DescribeStreamConsumerWithContext(ctx, &kinesis.DescribeStreamConsumerInput{
		ConsumerName: &a.metadata.ConsumerName,
		StreamARN:    streamARN,
	})
	cancel()
	if err != nil {
		return a.registerConsumer(parentCtx, streamARN)
	}

	return consumer.ConsumerDescription.ConsumerARN, nil
}

func (a *AWSKinesis) registerConsumer(ctx context.Context, streamARN *string) (*string, error) {
	consumer, err := a.client.RegisterStreamConsumerWithContext(ctx, &kinesis.RegisterStreamConsumerInput{
		ConsumerName: &a.metadata.ConsumerName,
		StreamARN:    streamARN,
	})
	if err != nil {
		return nil, err
	}

	err = a.waitUntilConsumerExists(ctx, &kinesis.DescribeStreamConsumerInput{
		ConsumerName: &a.metadata.ConsumerName,
		StreamARN:    streamARN,
	})

	if err != nil {
		return nil, err
	}

	return consumer.Consumer.ConsumerARN, nil
}

func (a *AWSKinesis) deregisterConsumer(streamARN *string, consumerARN *string) error {
	if a.consumerARN != nil {
		// Use a background context because the running context may have been canceled already
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, err := a.client.DeregisterStreamConsumerWithContext(ctx, &kinesis.DeregisterStreamConsumerInput{
			ConsumerARN:  consumerARN,
			StreamARN:    streamARN,
			ConsumerName: &a.metadata.ConsumerName,
		})
		cancel()

		return err
	}

	return nil
}

func (a *AWSKinesis) waitUntilConsumerExists(ctx aws.Context, input *kinesis.DescribeStreamConsumerInput, opts ...request.WaiterOption) error {
	w := request.Waiter{
		Name:        "WaitUntilConsumerExists",
		MaxAttempts: 18,
		Delay:       request.ConstantWaiterDelay(10 * time.Second),
		Acceptors: []request.WaiterAcceptor{
			{
				State:   request.SuccessWaiterState,
				Matcher: request.PathWaiterMatch, Argument: "ConsumerDescription.ConsumerStatus",
				Expected: "ACTIVE",
			},
		},
		NewRequest: func(opts []request.Option) (*request.Request, error) {
			var inCpy *kinesis.DescribeStreamConsumerInput
			if input != nil {
				tmp := *input
				inCpy = &tmp
			}
			req, _ := a.client.DescribeStreamConsumerRequest(inCpy)
			req.SetContext(ctx)
			req.ApplyOptions(opts...)

			return req, nil
		},
	}
	w.ApplyOptions(opts...)

	return w.WaitWithContext(ctx)
}

func (a *AWSKinesis) getClient(metadata *kinesisMetadata) (*kinesis.Kinesis, error) {
	sess, err := awsAuth.GetClient(metadata.AccessKey, metadata.SecretKey, metadata.SessionToken, metadata.Region, metadata.Endpoint)
	if err != nil {
		return nil, err
	}
	k := kinesis.New(sess)

	return k, nil
}

func (a *AWSKinesis) parseMetadata(meta bindings.Metadata) (*kinesisMetadata, error) {
	var m kinesisMetadata
	err := metadata.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func (a *AWSKinesis) recordProcessorFactory(ctx context.Context, handler bindings.Handler) interfaces.IRecordProcessorFactory {
	return &recordProcessorFactory{
		ctx:     ctx,
		logger:  a.logger,
		handler: handler,
	}
}

func (r *recordProcessorFactory) CreateProcessor() interfaces.IRecordProcessor {
	return &recordProcessor{
		ctx:     r.ctx,
		logger:  r.logger,
		handler: r.handler,
	}
}

func (p *recordProcessor) Initialize(input *interfaces.InitializationInput) {
	p.logger.Infof("Processing ShardId: %v at checkpoint: %v", input.ShardId, aws.StringValue(input.ExtendedSequenceNumber.SequenceNumber))
}

func (p *recordProcessor) ProcessRecords(input *interfaces.ProcessRecordsInput) {
	// don't process empty record
	if len(input.Records) == 0 {
		return
	}

	for _, v := range input.Records {
		p.handler(p.ctx, &bindings.ReadResponse{
			Data: v.Data,
		})
	}

	// checkpoint it after processing this batch
	lastRecordSequenceNumber := input.Records[len(input.Records)-1].SequenceNumber
	input.Checkpointer.Checkpoint(lastRecordSequenceNumber)
}

func (p *recordProcessor) Shutdown(input *interfaces.ShutdownInput) {
	if input.ShutdownReason == interfaces.TERMINATE {
		input.Checkpointer.Checkpoint(nil)
	}
}
