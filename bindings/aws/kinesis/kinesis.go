// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kinesis

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/google/uuid"
	"github.com/vmware/vmware-go-kcl/clientlibrary/config"
	"github.com/vmware/vmware-go-kcl/clientlibrary/interfaces"
	"github.com/vmware/vmware-go-kcl/clientlibrary/worker"

	aws_auth "github.com/dapr/components-contrib/authentication/aws"
	"github.com/dapr/components-contrib/bindings"
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
	StreamName          string              `json:"streamName"`
	ConsumerName        string              `json:"consumerName"`
	Region              string              `json:"region"`
	Endpoint            string              `json:"endpoint"`
	AccessKey           string              `json:"accessKey"`
	SecretKey           string              `json:"secretKey"`
	SessionToken        string              `json:"sessionToken"`
	KinesisConsumerMode kinesisConsumerMode `json:"mode"`
}

type kinesisConsumerMode string

const (
	// ExtendedFanout - dedicated throughput through data stream api.
	ExtendedFanout kinesisConsumerMode = "extended"

	// SharedThroughput - shared throughput using checkpoint and monitoring.
	SharedThroughput kinesisConsumerMode = "shared"

	partitionKeyName = "partitionKey"
)

// recordProcessorFactory.
type recordProcessorFactory struct {
	logger  logger.Logger
	handler func(*bindings.ReadResponse) ([]byte, error)
}

type recordProcessor struct {
	logger  logger.Logger
	handler func(*bindings.ReadResponse) ([]byte, error)
}

// NewAWSKinesis returns a new AWS Kinesis instance.
func NewAWSKinesis(logger logger.Logger) *AWSKinesis {
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

func (a *AWSKinesis) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	partitionKey := req.Metadata[partitionKeyName]
	if partitionKey == "" {
		partitionKey = uuid.New().String()
	}
	_, err := a.client.PutRecord(&kinesis.PutRecordInput{
		StreamName:   &a.metadata.StreamName,
		Data:         req.Data,
		PartitionKey: &partitionKey,
	})

	return nil, err
}

func (a *AWSKinesis) Read(handler func(*bindings.ReadResponse) ([]byte, error)) error {
	if a.metadata.KinesisConsumerMode == SharedThroughput {
		a.worker = worker.NewWorker(a.recordProcessorFactory(handler), a.workerConfig)
		err := a.worker.Start()
		if err != nil {
			return err
		}
	} else if a.metadata.KinesisConsumerMode == ExtendedFanout {
		ctx := context.Background()
		stream, err := a.client.DescribeStream(&kinesis.DescribeStreamInput{StreamName: &a.metadata.StreamName})
		if err != nil {
			return err
		}
		go a.Subscribe(ctx, *stream.StreamDescription, handler)
	}

	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, os.Interrupt, syscall.SIGTERM)
	<-exitChan

	if a.metadata.KinesisConsumerMode == SharedThroughput {
		go a.worker.Shutdown()
	} else if a.metadata.KinesisConsumerMode == ExtendedFanout {
		go a.deregisterConsumer(a.streamARN, a.consumerARN)
	}

	return nil
}

// Subscribe to all shards.
func (a *AWSKinesis) Subscribe(ctx context.Context, streamDesc kinesis.StreamDescription, handler func(*bindings.ReadResponse) ([]byte, error)) error {
	consumerARN, err := a.ensureConsumer(streamDesc.StreamARN)
	if err != nil {
		a.logger.Error(err)

		return err
	}

	a.consumerARN = consumerARN

	for {
		var wg sync.WaitGroup
		wg.Add(len(streamDesc.Shards))
		for i, shard := range streamDesc.Shards {
			go func(idx int, s *kinesis.Shard) error {
				defer wg.Done()
				sub, err := a.client.SubscribeToShardWithContext(ctx, &kinesis.SubscribeToShardInput{
					ConsumerARN:      consumerARN,
					ShardId:          s.ShardId,
					StartingPosition: &kinesis.StartingPosition{Type: aws.String(kinesis.ShardIteratorTypeLatest)},
				})
				if err != nil {
					a.logger.Error(err)

					return err
				}

				for event := range sub.EventStream.Events() {
					switch e := event.(type) {
					case *kinesis.SubscribeToShardEvent:
						for _, rec := range e.Records {
							handler(&bindings.ReadResponse{
								Data: rec.Data,
							})
						}
					}
				}

				return nil
			}(i, shard)
		}
		wg.Wait()
		time.Sleep(time.Minute * 5)
	}
}

func (a *AWSKinesis) ensureConsumer(streamARN *string) (*string, error) {
	consumer, err := a.client.DescribeStreamConsumer(&kinesis.DescribeStreamConsumerInput{
		ConsumerName: &a.metadata.ConsumerName,
		StreamARN:    streamARN,
	})
	if err != nil {
		arn, err := a.registerConsumer(streamARN)

		return arn, err
	}

	return consumer.ConsumerDescription.ConsumerARN, nil
}

func (a *AWSKinesis) registerConsumer(streamARN *string) (*string, error) {
	consumer, err := a.client.RegisterStreamConsumer(&kinesis.RegisterStreamConsumerInput{
		ConsumerName: &a.metadata.ConsumerName,
		StreamARN:    streamARN,
	})
	if err != nil {
		return nil, err
	}

	err = a.waitUntilConsumerExists(context.Background(), &kinesis.DescribeStreamConsumerInput{
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
		_, err := a.client.DeregisterStreamConsumer(&kinesis.DeregisterStreamConsumerInput{
			ConsumerARN:  consumerARN,
			StreamARN:    streamARN,
			ConsumerName: &a.metadata.ConsumerName,
		})

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
	sess, err := aws_auth.GetClient(metadata.AccessKey, metadata.SecretKey, metadata.SessionToken, metadata.Region, metadata.Endpoint)
	if err != nil {
		return nil, err
	}
	k := kinesis.New(sess)

	return k, nil
}

func (a *AWSKinesis) parseMetadata(metadata bindings.Metadata) (*kinesisMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var m kinesisMetadata
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (a *AWSKinesis) recordProcessorFactory(handler func(*bindings.ReadResponse) ([]byte, error)) interfaces.IRecordProcessorFactory {
	return &recordProcessorFactory{logger: a.logger, handler: handler}
}

func (r *recordProcessorFactory) CreateProcessor() interfaces.IRecordProcessor {
	return &recordProcessor{logger: r.logger, handler: r.handler}
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
		p.handler(&bindings.ReadResponse{
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
