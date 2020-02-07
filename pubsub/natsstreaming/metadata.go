// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package natsstreaming

import "time"

type metadata struct {
	natsURL                 string
	natsStreamingClusterID  string
	subscriptionType        string
	natsQueueGroupName      string
	durableSubscriptionName string
	startAtSequence         uint64
	startWithLastReceived   string
	deliverNew              string
	deliverAll              string
	startAtTimeDelta        time.Duration
	startAtTime             string
	startAtTimeFormat       string
}
