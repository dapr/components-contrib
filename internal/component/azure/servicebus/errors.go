package servicebus

import (
	"context"
	"errors"

	azservicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/go-amqp"
)

var retriableSendingErrors = map[amqp.ErrCond]struct{}{
	"com.microsoft:server-busy'":             {},
	amqp.ErrCondResourceLimitExceeded:        {},
	amqp.ErrCondResourceLocked:               {},
	amqp.ErrCondTransferLimitExceeded:        {},
	amqp.ErrCondInternalError:                {},
	amqp.ErrCondIllegalState:                 {},
	"com.microsoft:message-lock-lost":        {},
	"com.microsoft:session-cannot-be-locked": {},
	"com.microsoft:timeout":                  {},
	"com.microsoft:session-lock-lost":        {},
	"com.microsoft:store-lock-lost":          {},
}

// IsNetworkError returns true if the error returned by Service Bus is a network-level one, which would require reconnecting.
func IsNetworkError(err error) bool {
	if err == nil {
		return false
	}

	var expError *azservicebus.Error
	if errors.As(err, &expError) {
		if expError.Code == "connlost" {
			return true
		}
	}

	// Context deadline exceeded errors often happen when the connection is just "hanging"
	var connErr *amqp.ConnError
	if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &connErr) {
		return true
	}

	return false
}

// IsRetriableAMQPError returns true if the error returned by Service Bus is a retriable error from AMQP, which doesn't require reconnecting.
func IsRetriableAMQPError(err error) bool {
	var amqpError *amqp.Error
	if errors.As(err, &amqpError) {
		if _, ok := retriableSendingErrors[amqpError.Condition]; ok {
			return true
		}
	}
	return false
}
