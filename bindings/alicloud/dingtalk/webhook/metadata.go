// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

// DingTalk webhook are a simple way to post messages from apps into DingTalk
//
// See https://developers.dingtalk.com/document/app/custom-robot-access for details

package webhook

type metadata struct {
	id     string
	url    string
	secret string
}
