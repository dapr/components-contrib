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

package servicebus

import (
	"context"
	"fmt"

	azservicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"go.uber.org/multierr"
)

type Receiver interface {
	ReceiveMessages(ctx context.Context, maxMessages int, options *azservicebus.ReceiveMessagesOptions) ([]*azservicebus.ReceivedMessage, error)
	CompleteMessage(ctx context.Context, m *azservicebus.ReceivedMessage, opts *azservicebus.CompleteMessageOptions) error
	AbandonMessage(ctx context.Context, m *azservicebus.ReceivedMessage, opts *azservicebus.AbandonMessageOptions) error
	Close(ctx context.Context) error
}

var (
	_ Receiver = (*SessionReceiver)(nil)
	_ Receiver = (*MessageReceiver)(nil)
)

func NewSessionReceiver(r *azservicebus.SessionReceiver) *SessionReceiver {
	return &SessionReceiver{SessionReceiver: r}
}

type SessionReceiver struct {
	*azservicebus.SessionReceiver
}

func (s *SessionReceiver) RenewSessionLocks(ctx context.Context) error {
	if s == nil {
		return nil
	}

	return s.RenewSessionLock(ctx, nil)
}

func NewMessageReceiver(r *azservicebus.Receiver) *MessageReceiver {
	return &MessageReceiver{Receiver: r}
}

type MessageReceiver struct {
	*azservicebus.Receiver
}

func (m *MessageReceiver) RenewMessageLocks(ctx context.Context, msgs []*azservicebus.ReceivedMessage) error {
	if m == nil {
		return nil
	}

	var errs []error
	for _, msg := range msgs {
		// Renew the lock for the message.
		err := m.RenewMessageLock(ctx, msg, nil)
		if err != nil {
			errs = append(errs, fmt.Errorf("couldn't renew all active message lock(s) for message %s, %w", msg.MessageID, err))
		}
	}

	return multierr.Combine(errs...)
}
