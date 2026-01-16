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
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"github.com/vmware/vmware-go-kcl/clientlibrary/interfaces"
	"github.com/vmware/vmware-go-kcl/clientlibrary/worker"

	"github.com/dapr/components-contrib/bindings"
	awsAuth "github.com/dapr/components-contrib/common/authentication/aws"
	awsCommon "github.com/dapr/components-contrib/common/aws"
	awsCommonAuth "github.com/dapr/components-contrib/common/aws/auth"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

// AWSKinesis allows receiving and sending data to/from AWS Kinesis stream.
type AWSKinesis struct {
	authProvider awsAuth.Provider
	metadata     *kinesisMetadata

	worker *worker.Worker

	streamName   string
	consumerName string
	consumerARN  *string
	logger       logger.Logger
	consumerMode string

	kinesisClient *kinesis.Client

	closed  atomic.Bool
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// TODO: we need to clean up the metadata fields here and update this binding to use the builtin aws auth provider and reflect in metadata.yaml
type kinesisMetadata struct {
	StreamName          string `json:"streamName" mapstructure:"streamName"`
	ConsumerName        string `json:"consumerName" mapstructure:"consumerName"`
	Region              string `json:"region" mapstructure:"region"`
	Endpoint            string `json:"endpoint" mapstructure:"endpoint"`
	AccessKey           string `json:"accessKey" mapstructure:"accessKey"`
	SecretKey           string `json:"secretKey" mapstructure:"secretKey"`
	SessionToken        string `json:"sessionToken" mapstructure:"sessionToken"`
	KinesisConsumerMode string `json:"mode" mapstructure:"mode"`
}

const (
	// ExtendedFanout - dedicated throughput through data stream api.
	ExtendedFanout = "extended"

	// SharedThroughput - shared throughput using checkpoint and monitoring.
	SharedThroughput = "shared"

	partitionKeyName = "partitionKey" // TODO: mv to metadata field instead
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
	return &AWSKinesis{
		logger:  logger,
		closeCh: make(chan struct{}),
	}
}

// Init does metadata parsing and connection creation.
func (a *AWSKinesis) Init(ctx context.Context, metadata bindings.Metadata) error {
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

	a.consumerMode = m.KinesisConsumerMode
	a.streamName = m.StreamName
	a.consumerName = m.ConsumerName
	a.metadata = m

	opts := awsAuth.Options{
		Logger:       a.logger,
		Properties:   metadata.Properties,
		Region:       m.Region,
		AccessKey:    m.AccessKey,
		SecretKey:    m.SecretKey,
		SessionToken: "",
	}
	// extra configs needed per component type
	provider, err := awsAuth.NewProvider(ctx, opts, awsAuth.GetConfig(opts))
	if err != nil {
		return err
	}
	a.authProvider = provider

	configOpts := awsCommonAuth.Options{
		Logger:       a.logger,
		Properties:   metadata.Properties,
		Region:       m.Region,
		Endpoint:     m.Endpoint,
		AccessKey:    m.AccessKey,
		SecretKey:    m.SecretKey,
		SessionToken: m.SessionToken,
	}
	awsCfg, err := awsCommon.NewConfig(ctx, configOpts)
	if err != nil {
		return fmt.Errorf("error getting AWS config: %w", err)
	}
	a.kinesisClient = kinesis.NewFromConfig(awsCfg)
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
	_, err := a.kinesisClient.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamName:   aws.String(a.metadata.StreamName),
		Data:         req.Data,
		PartitionKey: aws.String(partitionKey),
	})

	return nil, err
}

func (a *AWSKinesis) Read(ctx context.Context, handler bindings.Handler) (err error) {
	if a.closed.Load() {
		return errors.New("binding is closed")
	}

	if a.metadata.KinesisConsumerMode == SharedThroughput {
		// Configure the KCL worker with custom endpoints for LocalStack
		config := a.authProvider.Kinesis().WorkerCfg(ctx, a.streamName, a.consumerName, a.consumerMode)
		if a.metadata.Endpoint != "" {
			config.KinesisEndpoint = a.metadata.Endpoint
			config.DynamoDBEndpoint = a.metadata.Endpoint
		}
		a.worker = worker.NewWorker(a.recordProcessorFactory(ctx, handler), config)
		err = a.worker.Start()
		if err != nil {
			return err
		}
	} else if a.metadata.KinesisConsumerMode == ExtendedFanout {
		var streamResp *kinesis.DescribeStreamOutput
		streamResp, err = a.kinesisClient.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: aws.String(a.metadata.StreamName)})
		if err != nil {
			return err
		}
		if streamResp.StreamDescription == nil {
			return fmt.Errorf("empty stream description for %s", a.metadata.StreamName)
		}
		err = a.Subscribe(ctx, *streamResp.StreamDescription, handler)
		if err != nil {
			return err
		}
	}

	stream, err := a.authProvider.Kinesis().Stream(ctx, a.streamName)
	if err != nil {
		return fmt.Errorf("failed to get kinesis stream arn: %v", err)
	}
	// Wait for context cancelation then stop
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		select {
		case <-ctx.Done():
		case <-a.closeCh:
		}
		if a.metadata.KinesisConsumerMode == SharedThroughput {
			a.worker.Shutdown()
		} else if a.metadata.KinesisConsumerMode == ExtendedFanout {
			a.deregisterConsumer(ctx, stream, a.consumerARN)
		}
	}()

	return nil
}

// Subscribe to all shards.
func (a *AWSKinesis) Subscribe(ctx context.Context, streamDesc types.StreamDescription,
	handler bindings.Handler,
) error {
	consumerARN, err := a.ensureConsumer(ctx, streamDesc.StreamARN)
	if err != nil {
		a.logger.Error(err)
		return err
	}

	a.consumerARN = consumerARN

	a.wg.Add(len(streamDesc.Shards))
	for i, shard := range streamDesc.Shards {
		go func(idx int, s types.Shard) {
			defer a.wg.Done()

			// Reconnection backoff
			bo := backoff.NewExponentialBackOff()
			bo.InitialInterval = 2 * time.Second

			// Repeat until context is canceled or binding closed.
			for {
				select {
				case <-ctx.Done():
					return
				case <-a.closeCh:
					return
				default:
				}
				sub, err := a.kinesisClient.SubscribeToShard(ctx, &kinesis.SubscribeToShardInput{
					ConsumerARN:      consumerARN,
					ShardId:          s.ShardId,
					StartingPosition: &types.StartingPosition{Type: types.ShardIteratorTypeLatest},
				})
				if err != nil {
					wait := bo.NextBackOff()
					a.logger.Errorf("Error while reading from shard %v: %v. Attempting to reconnect in %s...", s.ShardId, err, wait)
					select {
					case <-ctx.Done():
						return
					case <-time.After(wait):
						continue
					}
				}

				// Reset the backoff on connection success
				bo.Reset()

				// Process events
				for ev := range sub.GetStream().Events() {
					switch v := ev.(type) {
					case *types.SubscribeToShardEventStreamMemberSubscribeToShardEvent:
						if len(v.Value.Records) > 0 {
							for _, rec := range v.Value.Records {
								handler(ctx, &bindings.ReadResponse{Data: rec.Data})
							}
						}
					}
				}
			}
		}(i, shard)
	}

	return nil
}

func (a *AWSKinesis) Close() error {
	if a.closed.CompareAndSwap(false, true) {
		close(a.closeCh)
	}
	a.wg.Wait()
	if a.authProvider != nil {
		return a.authProvider.Close()
	}
	return nil
}

func (a *AWSKinesis) ensureConsumer(ctx context.Context, streamARN *string) (*string, error) {
	// Only set timeout on consumer call.
	conCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	consumer, err := a.kinesisClient.DescribeStreamConsumer(conCtx, &kinesis.DescribeStreamConsumerInput{
		ConsumerName: aws.String(a.metadata.ConsumerName),
		StreamARN:    streamARN,
	})
	if err != nil {
		return a.registerConsumer(ctx, streamARN)
	}

	if consumer.ConsumerDescription != nil {
		return consumer.ConsumerDescription.ConsumerARN, nil
	}
	return nil, errors.New("empty consumer description")
}

func (a *AWSKinesis) registerConsumer(ctx context.Context, streamARN *string) (*string, error) {
	consumer, err := a.kinesisClient.RegisterStreamConsumer(ctx, &kinesis.RegisterStreamConsumerInput{
		ConsumerName: aws.String(a.metadata.ConsumerName),
		StreamARN:    streamARN,
	})
	if err != nil {
		return nil, err
	}

	err = a.waitUntilConsumerExists(ctx, &kinesis.DescribeStreamConsumerInput{
		ConsumerName: aws.String(a.metadata.ConsumerName),
		StreamARN:    streamARN,
	})
	if err != nil {
		return nil, err
	}

	if consumer.Consumer != nil {
		return consumer.Consumer.ConsumerARN, nil
	}

	return nil, errors.New("empty consumer ARN after register")
}

func (a *AWSKinesis) deregisterConsumer(ctx context.Context, streamARN *string, consumerARN *string) error {
	if a.consumerARN != nil {
		// Use a background context because the running context may have been canceled already
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := a.kinesisClient.DeregisterStreamConsumer(ctx, &kinesis.DeregisterStreamConsumerInput{
			ConsumerARN:  consumerARN,
			StreamARN:    streamARN,
			ConsumerName: aws.String(a.metadata.ConsumerName),
		})

		return err
	}

	return nil
}

func (a *AWSKinesis) waitUntilConsumerExists(ctx context.Context, input *kinesis.DescribeStreamConsumerInput) error {
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 10 * time.Second
	bo.MaxElapsedTime = 3 * time.Minute

	pollCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	for {
		select {
		case <-pollCtx.Done():
			return fmt.Errorf("timed out waiting for consumer to become ACTIVE: %w", pollCtx.Err())
		default:
			resp, err := a.kinesisClient.DescribeStreamConsumer(ctx, input)
			if err == nil && resp.ConsumerDescription != nil && resp.ConsumerDescription.ConsumerStatus == types.ConsumerStatusActive {
				return nil
			}
			wait := bo.NextBackOff()
			if wait == backoff.Stop {
				return errors.New("consumer did not become active in time")
			}
			select {
			case <-pollCtx.Done():
				return fmt.Errorf("timed out waiting for consumer to become ACTIVE: %w", pollCtx.Err())
			case <-time.After(wait):
			}
		}
	}
}

func (a *AWSKinesis) parseMetadata(meta bindings.Metadata) (*kinesisMetadata, error) {
	var m kinesisMetadata
	err := kitmd.DecodeMetadata(meta.Properties, &m)
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
	p.logger.Infof("Processing ShardId: %v at checkpoint: %v", input.ShardId, aws.ToString(input.ExtendedSequenceNumber.SequenceNumber))
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

// GetComponentMetadata returns the metadata of the component.
func (a *AWSKinesis) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := &kinesisMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}
