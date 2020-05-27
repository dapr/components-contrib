// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mqtt

type metadata struct {
	url          string
	clientID     string
	qos          byte
	retain       bool
	cleanSession bool
}
