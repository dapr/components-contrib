// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

import (
	"fmt"
)

// pubsub.NotFoundError is returned by the runtime when the pubsub does not exist
type NotFoundError struct {
	pubSubName string
}

func (e NotFoundError) Error() string {
	return fmt.Sprintf("pubsub '%s' not found", e.pubSubName)
}

// pubsub.NotAllowedError is returned by the runtime when publishing is forbidden
type NotAllowedError struct {
	topic string
	id    string
}

func (e NotAllowedError) Error() string {
	return fmt.Sprintf("topic %s is not allowed for app id %s", e.topic, e.id)
}
