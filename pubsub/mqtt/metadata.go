// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mqtt

type metadata struct {
	// mandatory parameters
	url   string
	topic string
	// optional parameters
	clientID     string
	qos          byte
	retain       bool
	cleanSession bool
}
