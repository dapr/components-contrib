// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mqtt

type metadata struct {
	tlsCfg
	url          string
	clientID     string
	qos          byte
	retain       bool
	cleanSession bool
}

type tlsCfg struct {
	caCert     string
	clientCert string
	clientKey  string
}
