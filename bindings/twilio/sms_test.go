// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package twilio

import (
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"toNumber": "toNumber", "fromNumber": "fromNumber"}
	tw := NewSMS(logger.NewLogger("test"))
	err := tw.Init(m)
	assert.NotNil(t, err)
}

func TestParseDuration(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"toNumber": "toNumber", "fromNumber": "fromNumber",
		"accountSid": "accountSid", "authToken": "authToken", "timeout": "badtimeout"}
	tw := NewSMS(logger.NewLogger("test"))
	err := tw.Init(m)
	assert.NotNil(t, err)
}
