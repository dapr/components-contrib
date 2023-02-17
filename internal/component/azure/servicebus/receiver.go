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
	"time"

	azservicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
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

func (s *SessionReceiver) RenewSessionLocks(ctx context.Context, timeout time.Duration) error {
	if s == nil {
		return nil
	}

	lockCtx, lockCancel := context.WithTimeout(ctx, timeout)
	defer lockCancel()

	return s.RenewSessionLock(lockCtx, nil)
}

func NewMessageReceiver(r *azservicebus.Receiver) *MessageReceiver {
	return &MessageReceiver{Receiver: r}
}

type MessageReceiver struct {
	*azservicebus.Receiver
}
