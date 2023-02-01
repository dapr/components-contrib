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
	// As for checking the string value too... Seems that the go-amqp library (which is used by the Service Bus SDK) may return "context deadline exceeded" errors that don't pass the errors.Is(err, context.DeadlineExceeded) test.
	// There are signs of the above in the Azure Service Bus SDK too: https://github.com/Azure/azure-sdk-for-go/blob/sdk/messaging/azservicebus/v1.1.4/sdk/messaging/azservicebus/internal/errors.go#L113
	if errors.Is(err, amqp.ErrConnClosed) || errors.Is(err, context.DeadlineExceeded) || err.Error() == context.DeadlineExceeded.Error() {
		return true
	}

	return false
}
