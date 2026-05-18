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
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// ---------------------------------------------------------------------------
// Mock clients
// ---------------------------------------------------------------------------

// mockGoodClient: all records succeed.
type mockGoodClient struct {
	putCount    atomic.Int64
	batchCalls  atomic.Int64
	singleCalls atomic.Int64
}

func (m *mockGoodClient) PutRecord(_ context.Context, _ *firehose.PutRecordInput, _ ...func(*firehose.Options)) (*firehose.PutRecordOutput, error) {
	m.singleCalls.Add(1)
	return &firehose.PutRecordOutput{RecordId: aws.String("r-1")}, nil
}

func (m *mockGoodClient) PutRecordBatch(_ context.Context, in *firehose.PutRecordBatchInput, _ ...func(*firehose.Options)) (*firehose.PutRecordBatchOutput, error) {
	m.batchCalls.Add(1)
	n := int64(len(in.Records))
	m.putCount.Add(n)

	responses := make([]types.PutRecordBatchResponseEntry, len(in.Records))
	for i := range in.Records {
		responses[i] = types.PutRecordBatchResponseEntry{
			RecordId: aws.String(fmt.Sprintf("r-%d", i)),
		}
	}
	return &firehose.PutRecordBatchOutput{
		FailedPutCount:   aws.Int32(0),
		RequestResponses: responses,
	}, nil
}

// mockBadClient: every other record fails (odd indices).
type mockBadClient struct {
	putCount atomic.Int64
}

func (m *mockBadClient) PutRecord(_ context.Context, _ *firehose.PutRecordInput, _ ...func(*firehose.Options)) (*firehose.PutRecordOutput, error) {
	return nil, errors.New("use batch")
}

func (m *mockBadClient) PutRecordBatch(_ context.Context, in *firehose.PutRecordBatchInput, _ ...func(*firehose.Options)) (*firehose.PutRecordBatchOutput, error) {
	var failed int32
	responses := make([]types.PutRecordBatchResponseEntry, len(in.Records))
	for i := range in.Records {
		if i%2 == 0 {
			responses[i] = types.PutRecordBatchResponseEntry{
				RecordId: aws.String(fmt.Sprintf("r-%d", i)),
			}
			m.putCount.Add(1)
		} else {
			failed++
			responses[i] = types.PutRecordBatchResponseEntry{
				ErrorCode:    aws.String("ServiceUnavailableException"),
				ErrorMessage: aws.String("Error"),
			}
		}
	}
	return &firehose.PutRecordBatchOutput{
		FailedPutCount:   aws.Int32(failed),
		RequestResponses: responses,
	}, nil
}

// mockReallyBadClient: every record fails, every time.
type mockReallyBadClient struct{}

func (m *mockReallyBadClient) PutRecord(_ context.Context, _ *firehose.PutRecordInput, _ ...func(*firehose.Options)) (*firehose.PutRecordOutput, error) {
	return nil, errors.New("always fails")
}

func (m *mockReallyBadClient) PutRecordBatch(_ context.Context, in *firehose.PutRecordBatchInput, _ ...func(*firehose.Options)) (*firehose.PutRecordBatchOutput, error) {
	responses := make([]types.PutRecordBatchResponseEntry, len(in.Records))
	for i := range in.Records {
		responses[i] = types.PutRecordBatchResponseEntry{
			ErrorCode:    aws.String("InternalFailure"),
			ErrorMessage: aws.String("Error"),
		}
	}
	return &firehose.PutRecordBatchOutput{
		FailedPutCount:   aws.Int32(int32(len(in.Records))), //nolint:gosec // test data: record count never exceeds int32
		RequestResponses: responses,
	}, nil
}

// mockExceptionClient: always returns a transport-level error.
type mockExceptionClient struct {
	attempts atomic.Int64
}

func (m *mockExceptionClient) PutRecord(_ context.Context, _ *firehose.PutRecordInput, _ ...func(*firehose.Options)) (*firehose.PutRecordOutput, error) {
	m.attempts.Add(1)
	return nil, errors.New("connection refused")
}

func (m *mockExceptionClient) PutRecordBatch(_ context.Context, _ *firehose.PutRecordBatchInput, _ ...func(*firehose.Options)) (*firehose.PutRecordBatchOutput, error) {
	m.attempts.Add(1)
	return nil, errors.New("connection refused")
}

// capturingClient wraps another client and captures the delivery stream name.
type capturingClient struct {
	inner         firehoseClient
	captureStream *string
}

func (c *capturingClient) PutRecord(ctx context.Context, in *firehose.PutRecordInput, opts ...func(*firehose.Options)) (*firehose.PutRecordOutput, error) {
	*c.captureStream = aws.ToString(in.DeliveryStreamName)
	return c.inner.PutRecord(ctx, in, opts...)
}

func (c *capturingClient) PutRecordBatch(ctx context.Context, in *firehose.PutRecordBatchInput, opts ...func(*firehose.Options)) (*firehose.PutRecordBatchOutput, error) {
	*c.captureStream = aws.ToString(in.DeliveryStreamName)
	return c.inner.PutRecordBatch(ctx, in, opts...)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newTestBinding(client firehoseClient) *AWSFirehose {
	return &AWSFirehose{
		client: client,
		metadata: &firehoseMetadata{
			DeliveryStreamName:        "test-stream",
			MaxBatchSize:              500,
			ServiceUnavailableRetries: 10,
			ServiceUnavailableTimeout: 0,
			RecordRetryLimit:          100,
		},
		logger: logger.NewLogger("test"),
	}
}

func makeRecordPayloads(n int) []map[string]string {
	records := make([]map[string]string, n)
	for i := range records {
		records[i] = map[string]string{"Data": fmt.Sprintf("record-%d\n", i)}
	}
	return records
}

func marshalRecords(records []map[string]string) []byte {
	b, _ := json.Marshal(records)
	return b
}

// ---------------------------------------------------------------------------
// Tests: batchRecords
// ---------------------------------------------------------------------------

func TestBatchRecords_Empty(t *testing.T) {
	batches := batchRecords(nil, 500)
	assert.Nil(t, batches)
}

func TestBatchRecords_ExactlyMaxSize(t *testing.T) {
	records := make([]types.Record, 500)
	batches := batchRecords(records, 500)
	require.Len(t, batches, 1)
	assert.Len(t, batches[0], 500)
}

func TestBatchRecords_LessThanMaxSize(t *testing.T) {
	records := make([]types.Record, 499)
	batches := batchRecords(records, 500)
	require.Len(t, batches, 1)
	assert.Len(t, batches[0], 499)
}

func TestBatchRecords_MultipleWithRemainder(t *testing.T) {
	records := make([]types.Record, 1010)
	for i := range records {
		records[i] = types.Record{Data: []byte(strconv.Itoa(i))}
	}
	batches := batchRecords(records, 500)
	require.Len(t, batches, 3)
	assert.Len(t, batches[0], 500)
	assert.Len(t, batches[1], 500)
	assert.Len(t, batches[2], 10)
	assert.Equal(t, []byte("0"), batches[0][0].Data)
	assert.Equal(t, []byte("500"), batches[1][0].Data)
	assert.Equal(t, []byte("1000"), batches[2][0].Data)
}

// ---------------------------------------------------------------------------
// Tests: getRecordsToRetry
// ---------------------------------------------------------------------------

func TestGetRecordsToRetry_NoFailures(t *testing.T) {
	records := []types.Record{{Data: []byte("a")}, {Data: []byte("b")}, {Data: []byte("c")}}
	output := &firehose.PutRecordBatchOutput{
		FailedPutCount: aws.Int32(0),
		RequestResponses: []types.PutRecordBatchResponseEntry{
			{RecordId: aws.String("0")},
			{RecordId: aws.String("1")},
			{RecordId: aws.String("2")},
		},
	}
	retry := getRecordsToRetry(output, records)
	assert.Empty(t, retry)
}

func TestGetRecordsToRetry_SomeFailures(t *testing.T) {
	records := []types.Record{{Data: []byte("a")}, {Data: []byte("b")}, {Data: []byte("c")}}
	output := &firehose.PutRecordBatchOutput{
		FailedPutCount: aws.Int32(2),
		RequestResponses: []types.PutRecordBatchResponseEntry{
			{ErrorCode: aws.String("1"), ErrorMessage: aws.String("")},
			{RecordId: aws.String("3")},
			{ErrorCode: aws.String("2"), ErrorMessage: aws.String("")},
		},
	}
	retry := getRecordsToRetry(output, records)
	require.Len(t, retry, 2)
	assert.Equal(t, []byte("a"), retry[0].Data)
	assert.Equal(t, []byte("c"), retry[1].Data)
}

// ---------------------------------------------------------------------------
// Tests: PutRecord (create operation)
// ---------------------------------------------------------------------------

func TestPutRecord_Success(t *testing.T) {
	mock := &mockGoodClient{}
	b := newTestBinding(mock)

	resp, err := b.Invoke(t.Context(), &bindings.InvokeRequest{
		Operation: bindings.CreateOperation,
		Data:      []byte(`{"key":"value"}`),
	})
	require.NoError(t, err)
	assert.Equal(t, "r-1", resp.Metadata["recordId"])
	assert.Equal(t, int64(1), mock.singleCalls.Load())
}

func TestPutRecord_EmptyData(t *testing.T) {
	b := newTestBinding(&mockGoodClient{})
	_, err := b.Invoke(t.Context(), &bindings.InvokeRequest{
		Operation: bindings.CreateOperation,
		Data:      nil,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty data")
}

func TestPutRecord_ServiceRetry(t *testing.T) {
	mock := &mockExceptionClient{}
	b := newTestBinding(mock)
	b.metadata.ServiceUnavailableRetries = 2
	b.metadata.ServiceUnavailableTimeout = 0

	_, err := b.Invoke(t.Context(), &bindings.InvokeRequest{
		Operation: bindings.CreateOperation,
		Data:      []byte("test"),
	})
	require.Error(t, err)
	assert.Equal(t, int64(3), mock.attempts.Load())
	assert.Contains(t, err.Error(), "failed after 2 retries")
}

func TestPutRecord_PerRequestStreamOverride(t *testing.T) {
	var captured string
	mock := &mockGoodClient{}
	b := newTestBinding(mock)
	b.client = &capturingClient{
		inner:         mock,
		captureStream: &captured,
	}

	_, err := b.Invoke(t.Context(), &bindings.InvokeRequest{
		Operation: bindings.CreateOperation,
		Data:      []byte("test"),
		Metadata:  map[string]string{"deliveryStreamName": "override-stream"},
	})
	require.NoError(t, err)
	assert.Equal(t, "override-stream", captured)
}

// ---------------------------------------------------------------------------
// Tests: PutRecordBatch (batch operation)
// ---------------------------------------------------------------------------

func TestBatchInvoke_AllSuccess(t *testing.T) {
	mock := &mockGoodClient{}
	b := newTestBinding(mock)

	records := makeRecordPayloads(1025)
	resp, err := b.Invoke(t.Context(), &bindings.InvokeRequest{
		Operation: OperationBatch,
		Data:      marshalRecords(records),
	})
	require.NoError(t, err)
	assert.Equal(t, "1025", resp.Metadata["recordCount"])
	assert.Equal(t, int64(3), mock.batchCalls.Load())
	assert.Equal(t, int64(1025), mock.putCount.Load())
}

func TestBatchInvoke_PartialFailure_RetriesExhausted(t *testing.T) {
	mock := &mockBadClient{}
	b := newTestBinding(mock)
	b.metadata.RecordRetryLimit = 3

	records := makeRecordPayloads(1025)
	_, err := b.Invoke(t.Context(), &bindings.InvokeRequest{
		Operation: OperationBatch,
		Data:      marshalRecords(records),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "record retry limit")
}

func TestBatchInvoke_AllFail_RetriesExhausted(t *testing.T) {
	mock := &mockReallyBadClient{}
	b := newTestBinding(mock)
	b.metadata.RecordRetryLimit = 5

	records := makeRecordPayloads(10)
	_, err := b.Invoke(t.Context(), &bindings.InvokeRequest{
		Operation: OperationBatch,
		Data:      marshalRecords(records),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "record retry limit")
}

func TestBatchInvoke_ServiceException(t *testing.T) {
	mock := &mockExceptionClient{}
	b := newTestBinding(mock)
	b.metadata.ServiceUnavailableRetries = 2
	b.metadata.ServiceUnavailableTimeout = 0

	records := makeRecordPayloads(3)
	_, err := b.Invoke(t.Context(), &bindings.InvokeRequest{
		Operation: OperationBatch,
		Data:      marshalRecords(records),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed after 2 retries")
}

func TestBatchInvoke_Empty(t *testing.T) {
	mock := &mockGoodClient{}
	b := newTestBinding(mock)

	resp, err := b.Invoke(t.Context(), &bindings.InvokeRequest{
		Operation: OperationBatch,
		Data:      []byte("[]"),
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, int64(0), mock.batchCalls.Load())
}

func TestBatchInvoke_InvalidJSON(t *testing.T) {
	b := newTestBinding(&mockGoodClient{})
	_, err := b.Invoke(t.Context(), &bindings.InvokeRequest{
		Operation: OperationBatch,
		Data:      []byte("not json"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "JSON array")
}

// ---------------------------------------------------------------------------
// Tests: unmarshalBatchRecords
// ---------------------------------------------------------------------------

func TestUnmarshalBatchRecords_DataObjectFormat(t *testing.T) {
	input := `[{"Data": "line1\n"}, {"Data": "line2\n"}]`
	records, err := unmarshalBatchRecords([]byte(input))
	require.NoError(t, err)
	require.Len(t, records, 2)
	assert.Equal(t, []byte("line1\n"), records[0].Data)
	assert.Equal(t, []byte("line2\n"), records[1].Data)
}

func TestUnmarshalBatchRecords_RawFormat(t *testing.T) {
	input := `[42, "hello", {"nested": true}]`
	records, err := unmarshalBatchRecords([]byte(input))
	require.NoError(t, err)
	require.Len(t, records, 3)
	assert.Equal(t, []byte("42"), records[0].Data)
	assert.Equal(t, []byte(`"hello"`), records[1].Data)
	assert.Equal(t, []byte(`{"nested": true}`), records[2].Data)
}

// ---------------------------------------------------------------------------
// Tests: Operations
// ---------------------------------------------------------------------------

func TestOperations(t *testing.T) {
	b := newTestBinding(&mockGoodClient{})
	ops := b.Operations()
	assert.Contains(t, ops, bindings.CreateOperation)
	assert.Contains(t, ops, OperationBatch)
}

func TestUnsupportedOperation(t *testing.T) {
	b := newTestBinding(&mockGoodClient{})
	_, err := b.Invoke(t.Context(), &bindings.InvokeRequest{
		Operation: "delete",
		Data:      []byte("x"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported operation")
}

// ---------------------------------------------------------------------------
// Tests: Metadata parsing
// ---------------------------------------------------------------------------

func TestParseMetadata_Defaults(t *testing.T) {
	b := &AWSFirehose{logger: logger.NewLogger("test")}
	m, err := b.parseMetadata(bindings.Metadata{
		Base: contribMetadata.Base{
			Properties: map[string]string{
				"deliveryStreamName": "my-stream",
				"region":             "us-east-1",
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "my-stream", m.DeliveryStreamName)
	assert.Equal(t, "us-east-1", m.Region)
	assert.Equal(t, 500, m.MaxBatchSize)
	assert.Equal(t, 10, m.ServiceUnavailableRetries)
	assert.Equal(t, 60*time.Second, m.ServiceUnavailableTimeout)
	assert.Equal(t, 100, m.RecordRetryLimit)
}

func TestParseMetadata_CustomValues(t *testing.T) {
	b := &AWSFirehose{logger: logger.NewLogger("test")}
	m, err := b.parseMetadata(bindings.Metadata{
		Base: contribMetadata.Base{
			Properties: map[string]string{
				"deliveryStreamName":        "custom-stream",
				"region":                    "eu-west-1",
				"maxBatchSize":              "250",
				"serviceUnavailableRetries": "5",
				"serviceUnavailableTimeout": "30s",
				"recordRetryLimit":          "50",
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 250, m.MaxBatchSize)
	assert.Equal(t, 5, m.ServiceUnavailableRetries)
	assert.Equal(t, 30*time.Second, m.ServiceUnavailableTimeout)
	assert.Equal(t, 50, m.RecordRetryLimit)
}

func TestParseMetadata_InvalidBatchSize_ClampedToDefault(t *testing.T) {
	b := &AWSFirehose{logger: logger.NewLogger("test")}
	m, err := b.parseMetadata(bindings.Metadata{
		Base: contribMetadata.Base{
			Properties: map[string]string{
				"deliveryStreamName": "s",
				"maxBatchSize":       "9999",
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 500, m.MaxBatchSize)
}

// ---------------------------------------------------------------------------
// Tests: context cancellation during retry
// ---------------------------------------------------------------------------

func TestPutRecord_ContextCancelled(t *testing.T) {
	mock := &mockExceptionClient{}
	b := newTestBinding(mock)
	b.metadata.ServiceUnavailableTimeout = 5 * time.Second

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := b.Invoke(ctx, &bindings.InvokeRequest{
		Operation: bindings.CreateOperation,
		Data:      []byte("test"),
	})
	require.Error(t, err)
}

func TestMissingDeliveryStream(t *testing.T) {
	b := newTestBinding(&mockGoodClient{})
	b.metadata.DeliveryStreamName = ""

	_, err := b.Invoke(t.Context(), &bindings.InvokeRequest{
		Operation: bindings.CreateOperation,
		Data:      []byte("test"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deliveryStreamName is required")
}

// ---------------------------------------------------------------------------
// Tests: GetComponentMetadata
// ---------------------------------------------------------------------------

func TestGetComponentMetadata(t *testing.T) {
	b := &AWSFirehose{logger: logger.NewLogger("test")}
	md := b.GetComponentMetadata()
	assert.NotNil(t, md)
}
