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

package redis

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonredis "github.com/dapr/components-contrib/common/component/redis"
	mdata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

func getFakeProperties() map[string]string {
	return map[string]string{
		consumerID:   "fakeConsumer",
		enableTLS:    "true",
		maxLenApprox: "1000",
		streamTTL:    "1h",
	}
}

func TestParseRedisMetadata(t *testing.T) {
	t.Run("metadata is correct", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}

		// act
		m := commonredis.Settings{}
		err := kitmd.DecodeMetadata(fakeMetaData, &m)

		// assert
		require.NoError(t, err)
		assert.Equal(t, fakeProperties[consumerID], m.ConsumerID)
		assert.Equal(t, int64(1000), m.MaxLenApprox)
		assert.Equal(t, 1*time.Hour, m.StreamTTL)
	})

	// TODO: fix the code to return the error for the missing property to make this test work
	// t.Run("consumerID is not given", func(t *testing.T) {
	// 	fakeProperties := getFakeProperties()

	// 	fakeMetaData := pubsub.Metadata{
	// 		Base: mdata.Base{Properties: fakeProperties},
	// 	}
	// 	fakeMetaData.Properties[consumerID] = ""

	// 	// act
	// 	m := commonredis.Settings{}
	// 	err := kitmd.DecodeMetadata(fakeMetaData, &m)
	// 	// assert
	// 	require.ErrorIs(t, err, errors.New("redis streams error: missing consumerID"))
	// 	assert.Empty(t, m.ConsumerID)
	// })
}

func TestProcessStreams(t *testing.T) {
	fakeConsumerID := "fakeConsumer"
	messageCount := 0
	expectedData := "testData"
	expectedMetadata := "testMetadata"

	var wg sync.WaitGroup
	wg.Add(3)

	fakeHandler := func(ctx context.Context, msg *pubsub.NewMessage) error {
		defer wg.Done()

		messageCount++

		// assert
		assert.Equal(t, expectedData, string(msg.Data))
		assert.Equal(t, expectedMetadata, msg.Metadata["mymetadata"])

		// return fake error to skip executing redis client command
		return errors.New("fake error")
	}

	// act
	testRedisStream := &redisStreams{
		logger:         logger.NewLogger("test"),
		client:         &stubRedisClient{},
		clientSettings: &commonredis.Settings{ConsumerID: "group"},
	}
	testRedisStream.queue = make(chan redisMessageWrapper, 10)
	go testRedisStream.worker()
	testRedisStream.enqueueMessages(t.Context(), fakeConsumerID, fakeHandler, generateRedisStreamTestData(3, expectedData, expectedMetadata))

	// Wait for the handler to finish processing
	wg.Wait()

	// assert
	assert.Equal(t, 3, messageCount)
}

func TestProcessStreamsWithoutEventMetadata(t *testing.T) {
	fakeConsumerID := "fakeConsumer"
	messageCount := 0
	expectedData := "testData"

	var wg sync.WaitGroup
	wg.Add(3)

	fakeHandler := func(ctx context.Context, msg *pubsub.NewMessage) error {
		defer wg.Done()

		messageCount++

		// assert
		assert.Equal(t, expectedData, string(msg.Data))

		assert.Nil(t, msg.Metadata)

		// return fake error to skip executing redis client command
		return errors.New("fake error")
	}

	// act
	testRedisStream := &redisStreams{
		logger:         logger.NewLogger("test"),
		client:         &stubRedisClient{},
		clientSettings: &commonredis.Settings{ConsumerID: "group"},
	}
	testRedisStream.queue = make(chan redisMessageWrapper, 10)
	go testRedisStream.worker()
	testRedisStream.enqueueMessages(t.Context(), fakeConsumerID, fakeHandler, generateRedisStreamTestData(3, expectedData, ""))

	// Wait for the handler to finish processing
	wg.Wait()

	// assert
	assert.Equal(t, 3, messageCount)
}

func TestProcessMessageAcksOnError(t *testing.T) {
	client := &stubRedisClient{}
	rs := &redisStreams{
		logger: logger.NewLogger("test"),
		client: client,
		clientSettings: &commonredis.Settings{
			ConsumerID: "group",
		},
	}

	msg := redisMessageWrapper{
		ctx:       t.Context(),
		messageID: "1-0",
		message: pubsub.NewMessage{
			Topic: "topic",
		},
		handler: func(context.Context, *pubsub.NewMessage) error {
			return errors.New("retry")
		},
	}

	err := rs.processMessage(msg)

	require.NoError(t, err)
	assert.Equal(t, 1, client.ackCount)
	assert.Equal(t, "topic", client.ackStream)
	assert.Equal(t, "group", client.ackGroup)
	assert.Equal(t, "1-0", client.ackMessageID)
}

func TestProcessMessageAckFailureOnError(t *testing.T) {
	client := &stubRedisClient{
		ackErr: errors.New("ack-failed"),
	}
	rs := &redisStreams{
		logger: logger.NewLogger("test"),
		client: client,
		clientSettings: &commonredis.Settings{
			ConsumerID: "group",
		},
	}

	msg := redisMessageWrapper{
		ctx:       t.Context(),
		messageID: "1-0",
		message: pubsub.NewMessage{
			Topic: "topic",
		},
		handler: func(context.Context, *pubsub.NewMessage) error {
			return errors.New("retry")
		},
	}

	err := rs.processMessage(msg)

	require.Error(t, err)
	assert.Equal(t, client.ackErr, err)
	assert.Equal(t, 1, client.ackCount)
}

// TestReclaimPendingMessagesWhenXClaimReturnsRedisNil ensures that when XClaimResult returns
// redis "key does not exist" (redis: nil), we treat it as message no longer exists and call XAck
// instead of propagating the error to users.
func TestReclaimPendingMessagesWhenXClaimReturnsRedisNil(t *testing.T) {
	client := &stubRedisClient{
		nilValueErrStr: "redis: nil",
		pendingResult: []commonredis.RedisXPendingExt{
			{ID: "1-0", Consumer: "c", Idle: time.Minute, RetryCount: 0},
		},
		claimErr: commonredis.RedisError("redis: nil"),
	}
	rs := &redisStreams{
		logger: logger.NewLogger("test"),
		client: client,
		clientSettings: &commonredis.Settings{
			ConsumerID:        "group",
			ProcessingTimeout: time.Second,
			RedeliverInterval: time.Hour,
			QueueDepth:        10,
		},
	}

	rs.reclaimPendingMessages(t.Context(), "stream", func(ctx context.Context, msg *pubsub.NewMessage) error {
		return nil
	})

	// When XClaim returns redis nil, we ack the message to remove it from pending
	assert.GreaterOrEqual(t, client.ackCount, 1, "expected XAck to be called for message that no longer exists")
}

// TestReclaimPendingMessagesWhenXPendingExtReturnsRedisNil ensures that when XPendingExtResult
// returns redis "key does not exist" (redis: nil), we do not log it as an error and the loop exits
func TestReclaimPendingMessagesWhenXPendingExtReturnsRedisNil(t *testing.T) {
	client := &stubRedisClient{
		nilValueErrStr: "redis: nil",
		xPendingErr:    commonredis.RedisError("redis: nil"),
	}
	rs := &redisStreams{
		logger: logger.NewLogger("test"),
		client: client,
		clientSettings: &commonredis.Settings{
			ConsumerID:        "group",
			ProcessingTimeout: time.Second,
			RedeliverInterval: time.Hour,
			QueueDepth:        10,
		},
	}

	// treat as not pending and break when XPending returns redis nil
	rs.reclaimPendingMessages(t.Context(), "stream", func(ctx context.Context, msg *pubsub.NewMessage) error {
		return nil
	})
}

// TestPollNewMessagesLoopWhenXReadGroupResultReturnsRedisNil ensures that when XReadGroupResult
// returns redis "key does not exist" (redis: nil), we do not log it and the loop continues until ctx is canceled
func TestPollNewMessagesLoopWhenXReadGroupResultReturnsRedisNil(t *testing.T) {
	client := &stubRedisClient{
		nilValueErrStr: "redis: nil",
		readGroupErr:   commonredis.RedisError("redis: nil"),
	}
	rs := &redisStreams{
		logger: logger.NewLogger("test"),
		client: client,
		clientSettings: &commonredis.Settings{
			ConsumerID:  "group",
			QueueDepth:  10,
			ReadTimeout: commonredis.Duration(time.Second),
		},
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		rs.pollNewMessagesLoop(ctx, "stream", func(context.Context, *pubsub.NewMessage) error { return nil })
		close(done)
	}()
	time.Sleep(2 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pollNewMessagesLoop did not exit after context cancel")
	}
}

func generateRedisStreamTestData(messageCount int, data string, metadata string) []commonredis.RedisXMessage {
	generateXMessage := func(id int) commonredis.RedisXMessage {
		values := map[string]interface{}{
			"data": data,
		}

		if metadata != "" {
			values["metadata"] = "{\"mymetadata\": \"" + metadata + "\"}"
		}

		return commonredis.RedisXMessage{
			ID:     strconv.Itoa(id),
			Values: values,
		}
	}

	xmessageArray := make([]commonredis.RedisXMessage, messageCount)
	for i := range xmessageArray {
		xmessageArray[i] = generateXMessage(i)
	}

	return xmessageArray
}

type stubRedisClient struct {
	ackCount       int
	ackErr         error
	ackStream      string
	ackGroup       string
	ackMessageID   string
	nilValueErrStr string // default "nil", used "redis: nil" to test nil key handling
	pendingResult  []commonredis.RedisXPendingExt
	claimErr       error // if set, XClaimResult returns (nil, claimErr)
	xPendingErr    error // if set, XPendingExtResult returns (nil, xPendingErr)
	readGroupErr   error // if set, XReadGroupResult returns (nil, readGroupErr)
}

func (s *stubRedisClient) IsNilValueError(err error) bool {
	return err != nil && err.Error() == s.nilValueErrStr
}

func (s *stubRedisClient) Context() context.Context {
	return context.Background()
}

func (s *stubRedisClient) DoRead(context.Context, ...interface{}) (interface{}, error) {
	return nil, nil
}

func (s *stubRedisClient) DoWrite(context.Context, ...interface{}) error {
	return nil
}

func (s *stubRedisClient) Del(context.Context, ...string) error {
	return nil
}

func (s *stubRedisClient) Get(context.Context, string) (string, error) {
	return "", nil
}

func (s *stubRedisClient) GetDel(context.Context, string) (string, error) {
	return "", nil
}

func (s *stubRedisClient) Close() error {
	return nil
}

func (s *stubRedisClient) PingResult(context.Context) (string, error) {
	return "", nil
}

func (s *stubRedisClient) ConfigurationSubscribe(context.Context, *commonredis.ConfigurationSubscribeArgs) {
}

func (s *stubRedisClient) SetNX(context.Context, string, interface{}, time.Duration) (*bool, error) {
	return nil, nil
}

func (s *stubRedisClient) EvalInt(context.Context, string, []string, ...interface{}) (*int, error, error) {
	i := 0
	return &i, nil, nil
}

func (s *stubRedisClient) XAdd(context.Context, string, int64, string, map[string]interface{}) (string, error) {
	return "", nil
}

func (s *stubRedisClient) XGroupCreateMkStream(context.Context, string, string, string) error {
	return nil
}

func (s *stubRedisClient) XAck(ctx context.Context, stream string, group string, messageID string) error {
	s.ackCount++
	s.ackStream = stream
	s.ackGroup = group
	s.ackMessageID = messageID
	return s.ackErr
}

func (s *stubRedisClient) XReadGroupResult(context.Context, string, string, []string, int64, time.Duration) ([]commonredis.RedisXStream, error) {
	if s.readGroupErr != nil {
		return nil, s.readGroupErr
	}
	return nil, nil
}

func (s *stubRedisClient) XPendingExtResult(context.Context, string, string, string, string, int64) ([]commonredis.RedisXPendingExt, error) {
	if s.xPendingErr != nil {
		return nil, s.xPendingErr
	}
	if s.pendingResult != nil {
		result := s.pendingResult
		s.pendingResult = nil // so next call returns empty and reclaimPendingMessages loop exits
		return result, nil
	}
	return nil, nil
}

func (s *stubRedisClient) XClaimResult(context.Context, string, string, string, time.Duration, []string) ([]commonredis.RedisXMessage, error) {
	if s.claimErr != nil {
		return nil, s.claimErr
	}
	return nil, nil
}

func (s *stubRedisClient) TxPipeline() commonredis.RedisPipeliner {
	return &stubRedisPipeliner{}
}

func (s *stubRedisClient) TTLResult(context.Context, string) (time.Duration, error) {
	return 0, nil
}

func (s *stubRedisClient) AuthACL(context.Context, string, string) error {
	return nil
}

type stubRedisPipeliner struct{}

func (p *stubRedisPipeliner) Exec(context.Context) error {
	return nil
}

func (p *stubRedisPipeliner) Do(context.Context, ...interface{}) {}
