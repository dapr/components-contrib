// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package azureservicebus

type metadata struct {
	ConnectionString string `json:"connectionString"`
	ConsumerID       string `json:"consumerID"`
	MaxDeliveryCount int    `json:"maxDeliveryCount"`
	TimeoutInSec     int    `json:"timeoutInSec"`
}
