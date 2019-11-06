// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"github.com/dapr/components-contrib/secretstores"
)

type metadata struct {
	host       string
	password   secretstores.SecretKey
	consumerID string
	enableTLS  bool
}
