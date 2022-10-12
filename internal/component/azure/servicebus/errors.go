package servicebus

import (
	"context"
	"errors"

	azservicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/go-amqp"
)

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
	if errors.Is(err, amqp.ErrConnClosed) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	return false
}
