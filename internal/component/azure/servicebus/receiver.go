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
