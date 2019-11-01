// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package azureservicebus

// TODO: Expose subscription options
type metadata struct {
	ConnectionString string `json:"connectionString"`
	ConsumerID       string `json:"consumerID"`
}