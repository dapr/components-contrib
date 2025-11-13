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

package topics

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	azservicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	impl "github.com/dapr/components-contrib/common/component/azure/servicebus"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

type mockReceiver struct {
	messages     []*azservicebus.ReceivedMessage
	messageIndex int
	sessionID    string
	mu           sync.Mutex
	closed       bool
}

func newMockReceiver(sessionID string, messages []*azservicebus.ReceivedMessage) *mockReceiver {
	return &mockReceiver{
		sessionID: sessionID,
		messages:  messages,
	}
}

func (m *mockReceiver) ReceiveMessages(ctx context.Context, count int, options *azservicebus.ReceiveMessagesOptions) ([]*azservicebus.ReceivedMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil, errors.New("receiver closed")
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if m.messageIndex >= len(m.messages) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
			return nil, errors.New("no more messages")
		}
	}

	end := m.messageIndex + count
	if end > len(m.messages) {
		end = len(m.messages)
	}

	result := m.messages[m.messageIndex:end]
	m.messageIndex = end
	return result, nil
}

func (m *mockReceiver) CompleteMessage(ctx context.Context, message *azservicebus.ReceivedMessage, options *azservicebus.CompleteMessageOptions) error {
	return nil
}

func (m *mockReceiver) AbandonMessage(ctx context.Context, message *azservicebus.ReceivedMessage, options *azservicebus.AbandonMessageOptions) error {
	return nil
}

func (m *mockReceiver) Close(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func TestSessionOrderingWithSingleHandler(t *testing.T) {
	const numMessages = 10
	sessionID := "test-session-1"

	messages := make([]*azservicebus.ReceivedMessage, numMessages)
	for i := range numMessages {
		seqNum := int64(i + 1)
		messages[i] = &azservicebus.ReceivedMessage{
			MessageID:      fmt.Sprintf("msg-%d", i),
			SessionID:      &sessionID,
			SequenceNumber: &seqNum,
			Body:           []byte(fmt.Sprintf("message-%d", i)),
		}
	}

	sub := impl.NewSubscription(
		impl.SubscriptionOptions{
			MaxActiveMessages:     100,
			TimeoutInSec:          5,
			MaxBulkSubCount:       ptr.Of(1),
			MaxConcurrentHandlers: 1,
			Entity:                "test-topic",
			LockRenewalInSec:      30,
			RequireSessions:       true,
			SessionIdleTimeout:    time.Second * 5,
		},
		logger.NewLogger("test"),
	)

	var (
		processedOrder []int
		orderMu        sync.Mutex
	)

	handlerFunc := func(ctx context.Context, msgs []*azservicebus.ReceivedMessage) ([]impl.HandlerResponseItem, error) {
		var msgIndex int
		_, err := fmt.Sscanf(string(msgs[0].Body), "message-%d", &msgIndex)
		require.NoError(t, err)

		time.Sleep(10 * time.Millisecond)

		orderMu.Lock()
		processedOrder = append(processedOrder, msgIndex)
		orderMu.Unlock()

		return nil, nil
	}

	receiver := newMockReceiver(sessionID, messages)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sub.ReceiveBlocking(ctx, handlerFunc, receiver, func() {}, "test-session")
	}()

	<-done

	expectedOrder := make([]int, numMessages)
	for i := range expectedOrder {
		expectedOrder[i] = i
	}

	assert.Equal(t, expectedOrder, processedOrder, "messages must be processed in order")
}

func TestMultipleSessionsConcurrentHandler(t *testing.T) {
	const (
		numSessions        = 5
		messagesPerSession = 10
		maxConcurrentLimit = 3
	)

	sessionIDs := make([]string, numSessions)
	for i := range numSessions {
		sessionIDs[i] = fmt.Sprintf("session-%d", i)
	}

	allMessages := make(map[string][]*azservicebus.ReceivedMessage)
	for _, sessionID := range sessionIDs {
		messages := make([]*azservicebus.ReceivedMessage, messagesPerSession)
		for i := range messagesPerSession {
			seqNum := int64(i + 1)
			sessID := sessionID
			messages[i] = &azservicebus.ReceivedMessage{
				MessageID:      fmt.Sprintf("%s-msg-%d", sessionID, i),
				SessionID:      &sessID,
				SequenceNumber: &seqNum,
				Body:           []byte(fmt.Sprintf("%s:%d", sessionID, i)),
			}
		}
		allMessages[sessionID] = messages
	}

	sub := impl.NewSubscription(
		impl.SubscriptionOptions{
			MaxActiveMessages:     100,
			TimeoutInSec:          5,
			MaxBulkSubCount:       ptr.Of(1),
			MaxConcurrentHandlers: maxConcurrentLimit,
			Entity:                "test-topic",
			LockRenewalInSec:      30,
			RequireSessions:       true,
			SessionIdleTimeout:    time.Second * 5,
		},
		logger.NewLogger("test"),
	)

	var (
		mu                    sync.Mutex
		globalOrder           []string // tracks session IDs in the order messages were received
		sessionOrders         = make(map[string][]int)
		concurrentHandlers    atomic.Int32
		maxConcurrentHandlers atomic.Int32
	)

	handlerFunc := func(ctx context.Context, msgs []*azservicebus.ReceivedMessage) ([]impl.HandlerResponseItem, error) {
		msg := msgs[0]
		sessionID := *msg.SessionID

		current := concurrentHandlers.Add(1)
		defer concurrentHandlers.Add(-1)

		for {
			max := maxConcurrentHandlers.Load()
			if current <= max || maxConcurrentHandlers.CompareAndSwap(max, current) {
				break
			}
		}

		var msgIndex int
		parts := strings.Split(string(msg.Body), ":")
		require.Len(t, parts, 2)
		_, err := fmt.Sscanf(parts[1], "%d", &msgIndex)
		require.NoError(t, err)

		time.Sleep(50 * time.Millisecond)

		mu.Lock()
		globalOrder = append(globalOrder, sessionID)
		sessionOrders[sessionID] = append(sessionOrders[sessionID], msgIndex)
		mu.Unlock()

		return nil, nil
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for _, sessionID := range sessionIDs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			receiver := newMockReceiver(sessionID, allMessages[sessionID])
			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = sub.ReceiveBlocking(ctx, handlerFunc, receiver, func() {}, "session-"+sessionID)
			}()
			<-done
		}()
	}

	wg.Wait()

	// Verify FIFO ordering per session
	for _, sessionID := range sessionIDs {
		order := sessionOrders[sessionID]
		require.Len(t, order, messagesPerSession, "session %s should process all messages", sessionID)

		for i := 0; i < messagesPerSession; i++ {
			assert.Equal(t, i, order[i], "session %s message %d out of order", sessionID, i)
		}
	}

	// Verify concurrent handler limits
	assert.LessOrEqual(t, maxConcurrentHandlers.Load(), int32(maxConcurrentLimit),
		"concurrent handlers should not exceed configured maximum")

	assert.Greater(t, maxConcurrentHandlers.Load(), int32(1),
		"multiple handlers should run concurrently across sessions")

	// Check global order to prove concurrent processing
	// If processed sequentially, all messages from one session would come before the next
	// If processed concurrently, session IDs will be interleaved
	hasInterleaving := false
	seenSessions := make(map[string]bool)
	lastSession := ""

	for _, sessionID := range globalOrder {
		if sessionID != lastSession && seenSessions[sessionID] {
			// We've seen this session before but with a different session in between
			hasInterleaving = true
			break
		}
		seenSessions[sessionID] = true
		lastSession = sessionID
	}

	assert.True(t, hasInterleaving,
		"global order must show session interleaving, proving concurrent processing")
}
