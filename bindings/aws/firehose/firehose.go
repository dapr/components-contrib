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

package firehose

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"

	"github.com/dapr/components-contrib/bindings"
	awsCommon "github.com/dapr/components-contrib/common/aws"
	awsAuth "github.com/dapr/components-contrib/common/aws/auth"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	// Default values.
	defaultMaxBatchSize              = 500
	defaultServiceUnavailableRetries = 10
	defaultServiceUnavailableTimeout = 60 * time.Second
	defaultRecordRetryLimit          = 100

	// AWS Firehose hard limits.
	maxRecordsPerBatch = 500

	// OperationBatch is the operation name for batch writes via PutRecordBatch.
	OperationBatch bindings.OperationKind = "batch"
)

// AWSFirehose is a Dapr output binding for AWS Kinesis Data Firehose.
// It supports single-record writes (PutRecord) via the "create" operation
// and batch writes (PutRecordBatch) via the "batch" operation, with automatic
// retry for both service-level and record-level failures.
type AWSFirehose struct {
	client   firehoseClient
	metadata *firehoseMetadata
	logger   logger.Logger
}

// firehoseClient abstracts the AWS Firehose SDK calls for testability.
type firehoseClient interface {
	PutRecord(ctx context.Context, params *firehose.PutRecordInput, optFns ...func(*firehose.Options)) (*firehose.PutRecordOutput, error)
	PutRecordBatch(ctx context.Context, params *firehose.PutRecordBatchInput, optFns ...func(*firehose.Options)) (*firehose.PutRecordBatchOutput, error)
}

type firehoseMetadata struct {
	// Ignored by metadata parser because included in built-in authentication profile.
	Region       string `json:"region" mapstructure:"region" mapstructurealiases:"awsRegion" mdignore:"true"`
	AccessKey    string `json:"accessKey" mapstructure:"accessKey" mdignore:"true"`
	SecretKey    string `json:"secretKey" mapstructure:"secretKey" mdignore:"true"`
	SessionToken string `json:"sessionToken" mapstructure:"sessionToken" mdignore:"true"`
	Endpoint     string `json:"endpoint" mapstructure:"endpoint"`

	// DeliveryStreamName is the default Firehose delivery stream name.
	// Can be overridden per-request via invoke metadata.
	DeliveryStreamName string `json:"deliveryStreamName" mapstructure:"deliveryStreamName"`

	// MaxBatchSize is the maximum number of records per PutRecordBatch call (max 500).
	MaxBatchSize int `json:"maxBatchSize" mapstructure:"maxBatchSize"`

	// ServiceUnavailableRetries is the number of retries on transient service errors.
	ServiceUnavailableRetries int `json:"serviceUnavailableRetries" mapstructure:"serviceUnavailableRetries"`

	// ServiceUnavailableTimeout is the wait duration between service-level retries.
	ServiceUnavailableTimeout time.Duration `json:"serviceUnavailableTimeout" mapstructure:"serviceUnavailableTimeout"`

	// RecordRetryLimit is the maximum number of rounds that failed records are re-batched and retried.
	RecordRetryLimit int `json:"recordRetryLimit" mapstructure:"recordRetryLimit"`
}

// NewAWSFirehose creates a new AWSFirehose output binding instance.
func NewAWSFirehose(logger logger.Logger) bindings.OutputBinding {
	return &AWSFirehose{logger: logger}
}

// Init parses metadata and creates the AWS Firehose client.
func (a *AWSFirehose) Init(ctx context.Context, md bindings.Metadata) error {
	m, err := a.parseMetadata(md)
	if err != nil {
		return fmt.Errorf("firehose binding init: %w", err)
	}
	a.metadata = m

	authOpts := awsAuth.Options{
		Logger:       a.logger,
		Properties:   md.Properties,
		Region:       m.Region,
		Endpoint:     m.Endpoint,
		AccessKey:    m.AccessKey,
		SecretKey:    m.SecretKey,
		SessionToken: m.SessionToken,
	}

	awsCfg, err := awsCommon.NewConfig(ctx, authOpts)
	if err != nil {
		return fmt.Errorf("firehose binding: error getting AWS config: %w", err)
	}

	a.client = firehose.NewFromConfig(awsCfg)
	return nil
}

// Operations returns the list of supported binding operations.
func (a *AWSFirehose) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.CreateOperation, // PutRecord — single record
		OperationBatch,           // PutRecordBatch — multiple records with retry
	}
}

// Invoke handles an invocation request.
//
// Operation "create":
//
//	req.Data is the raw record payload (bytes). Sent via PutRecord.
//	Per-request metadata key "deliveryStreamName" overrides the component default.
//
// Operation "batch":
//
//	req.Data is a JSON array of records. Each element is either:
//	  - An object with a "Data" string field (e.g. {"Data": "..."})
//	  - A raw JSON value used verbatim as record bytes
//	Records are batched (max 500), sent via PutRecordBatch, and failed records
//	are automatically retried up to RecordRetryLimit times.
func (a *AWSFirehose) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	deliveryStream := a.metadata.DeliveryStreamName
	if v, ok := req.Metadata["deliveryStreamName"]; ok && v != "" {
		deliveryStream = v
	}
	if deliveryStream == "" {
		return nil, errors.New("firehose binding: deliveryStreamName is required (set in component metadata or per-request)")
	}

	switch req.Operation {
	case bindings.CreateOperation:
		return a.putRecord(ctx, deliveryStream, req.Data)
	case OperationBatch:
		return a.putRecordBatch(ctx, deliveryStream, req.Data)
	default:
		return nil, fmt.Errorf("firehose binding: unsupported operation %q", req.Operation)
	}
}

// Close is a no-op; the AWS SDK client does not require explicit cleanup.
func (a *AWSFirehose) Close() error {
	return nil
}

// GetComponentMetadata returns metadata field information for documentation tooling.
func (a *AWSFirehose) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := firehoseMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}

// ---------------------------------------------------------------------------
// PutRecord — single record
// ---------------------------------------------------------------------------

func (a *AWSFirehose) putRecord(ctx context.Context, stream string, data []byte) (*bindings.InvokeResponse, error) {
	if len(data) == 0 {
		return nil, errors.New("firehose PutRecord: empty data")
	}

	input := &firehose.PutRecordInput{
		DeliveryStreamName: aws.String(stream),
		Record:             &types.Record{Data: data},
	}

	var lastErr error
	for attempt := 0; attempt <= a.metadata.ServiceUnavailableRetries; attempt++ {
		if attempt > 0 {
			a.logger.Warnf("PutRecord retry %d/%d after error: %v", attempt, a.metadata.ServiceUnavailableRetries, lastErr)
			if err := sleepContext(ctx, a.metadata.ServiceUnavailableTimeout); err != nil {
				return nil, err
			}
		}

		output, err := a.client.PutRecord(ctx, input)
		if err != nil {
			lastErr = err
			continue
		}

		return &bindings.InvokeResponse{
			Metadata: map[string]string{
				"recordId": aws.ToString(output.RecordId),
			},
		}, nil
	}

	return nil, fmt.Errorf("firehose PutRecord failed after %d retries: %w",
		a.metadata.ServiceUnavailableRetries, lastErr)
}

// ---------------------------------------------------------------------------
// PutRecordBatch — multiple records with batching + record-level retry
// ---------------------------------------------------------------------------

func (a *AWSFirehose) putRecordBatch(ctx context.Context, stream string, data []byte) (*bindings.InvokeResponse, error) {
	records, err := unmarshalBatchRecords(data)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return &bindings.InvokeResponse{}, nil
	}

	totalInputRecords := len(records)

	// Split into batches of maxBatchSize.
	pendingBatches := batchRecords(records, a.metadata.MaxBatchSize)
	retryRound := 0

	// Send each batch; collect failed records for retry.
	for len(pendingBatches) > 0 {
		var nextRoundBatches [][]types.Record

		for _, batch := range pendingBatches {
			retryRecords, batchErr := a.sendBatchWithServiceRetry(ctx, stream, batch)
			if batchErr != nil {
				return nil, batchErr
			}
			if len(retryRecords) > 0 {
				retryRound++
				if retryRound >= a.metadata.RecordRetryLimit {
					return nil, fmt.Errorf("firehose batch: record retry limit (%d) reached, giving up",
						a.metadata.RecordRetryLimit)
				}
				nextRoundBatches = append(nextRoundBatches, batchRecords(retryRecords, a.metadata.MaxBatchSize)...)
			}
		}

		pendingBatches = nextRoundBatches
	}

	return &bindings.InvokeResponse{
		Metadata: map[string]string{
			"recordCount": strconv.Itoa(totalInputRecords),
		},
	}, nil
}

// sendBatchWithServiceRetry calls PutRecordBatch with service-level retry on transient errors.
func (a *AWSFirehose) sendBatchWithServiceRetry(ctx context.Context, stream string, records []types.Record) ([]types.Record, error) {
	input := &firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String(stream),
		Records:            records,
	}

	var lastErr error
	for attempt := 0; attempt <= a.metadata.ServiceUnavailableRetries; attempt++ {
		if attempt > 0 {
			a.logger.Warnf("PutRecordBatch service retry %d/%d: %v",
				attempt, a.metadata.ServiceUnavailableRetries, lastErr)
			if err := sleepContext(ctx, a.metadata.ServiceUnavailableTimeout); err != nil {
				return nil, err
			}
		}

		output, err := a.client.PutRecordBatch(ctx, input)
		if err != nil {
			lastErr = err
			continue
		}

		return getRecordsToRetry(output, records), nil
	}

	return nil, fmt.Errorf("firehose PutRecordBatch failed after %d retries: %w",
		a.metadata.ServiceUnavailableRetries, lastErr)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// unmarshalBatchRecords parses a JSON array into Firehose Record objects.
// Supports {"Data": "..."} format as well as raw JSON values.
func unmarshalBatchRecords(data []byte) ([]types.Record, error) {
	var rawRecords []json.RawMessage
	if err := json.Unmarshal(data, &rawRecords); err != nil {
		return nil, fmt.Errorf("firehose batch: data must be a JSON array of records: %w", err)
	}

	records := make([]types.Record, 0, len(rawRecords))
	for _, raw := range rawRecords {
		var wrapper struct {
			Data string `json:"Data"`
		}
		if err := json.Unmarshal(raw, &wrapper); err == nil && wrapper.Data != "" {
			records = append(records, types.Record{Data: []byte(wrapper.Data)})
		} else {
			records = append(records, types.Record{Data: raw})
		}
	}
	return records, nil
}

// batchRecords splits a slice of records into sub-slices of at most maxSize.
func batchRecords(records []types.Record, maxSize int) [][]types.Record {
	if len(records) == 0 {
		return nil
	}
	batches := make([][]types.Record, 0, (len(records)+maxSize-1)/maxSize)
	for i := 0; i < len(records); i += maxSize {
		end := i + maxSize
		if end > len(records) {
			end = len(records)
		}
		batch := make([]types.Record, end-i)
		copy(batch, records[i:end])
		batches = append(batches, batch)
	}
	return batches
}

// getRecordsToRetry extracts the records that failed from a PutRecordBatch response.
func getRecordsToRetry(output *firehose.PutRecordBatchOutput, records []types.Record) []types.Record {
	if output.FailedPutCount == nil || *output.FailedPutCount == 0 {
		return nil
	}

	retry := make([]types.Record, 0, int(*output.FailedPutCount))
	for i, resp := range output.RequestResponses {
		if resp.ErrorCode != nil && *resp.ErrorCode != "" {
			retry = append(retry, records[i])
		}
	}
	return retry
}

// parseMetadata decodes and validates component metadata.
func (a *AWSFirehose) parseMetadata(meta bindings.Metadata) (*firehoseMetadata, error) {
	m := &firehoseMetadata{
		MaxBatchSize:              defaultMaxBatchSize,
		ServiceUnavailableRetries: defaultServiceUnavailableRetries,
		ServiceUnavailableTimeout: defaultServiceUnavailableTimeout,
		RecordRetryLimit:          defaultRecordRetryLimit,
	}

	if err := kitmd.DecodeMetadata(meta.Properties, m); err != nil {
		return nil, err
	}

	if m.MaxBatchSize <= 0 || m.MaxBatchSize > maxRecordsPerBatch {
		m.MaxBatchSize = defaultMaxBatchSize
	}
	if m.ServiceUnavailableRetries < 0 {
		m.ServiceUnavailableRetries = defaultServiceUnavailableRetries
	}
	if m.RecordRetryLimit <= 0 {
		m.RecordRetryLimit = defaultRecordRetryLimit
	}

	return m, nil
}

// sleepContext sleeps for d or returns early if ctx is cancelled.
func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
