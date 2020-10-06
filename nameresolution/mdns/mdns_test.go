// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mdns

import (
	"testing"

	nr "github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInit(t *testing.T) {
	tests := []struct {
		missingProp string
		props       map[string]string
	}{
		{
			"name",
			map[string]string{
				nr.MDNSInstanceAddress: "127.0.0.1",
				nr.MDNSInstancePort:    "30003",
			},
		},
		{
			"address",
			map[string]string{
				nr.MDNSInstanceName: "testAppID",
				nr.MDNSInstancePort: "30003",
			},
		},
		{
			"port",
			map[string]string{
				nr.MDNSInstanceName:    "testAppID",
				nr.MDNSInstanceAddress: "127.0.0.1",
			},
		},
		{
			"port",
			map[string]string{
				nr.MDNSInstanceName:    "testAppID",
				nr.MDNSInstanceAddress: "127.0.0.1",
				nr.MDNSInstancePort:    "abcd",
			},
		},
	}

	// arrange
	resolver := NewResolver(logger.NewLogger("test"))

	for _, tt := range tests {
		t.Run(tt.missingProp+" is missing", func(t *testing.T) {
			// act
			err := resolver.Init(nr.Metadata{Properties: tt.props})

			// assert
			assert.Error(t, err)
		})
	}
}

func TestResolver(t *testing.T) {
	// arrange
	resolver := NewResolver(logger.NewLogger("test"))
	md := nr.Metadata{Properties: map[string]string{
		nr.MDNSInstanceName:    "testAppID",
		nr.MDNSInstanceAddress: "127.0.0.1",
		nr.MDNSInstancePort:    "1234",
	}}

	// act
	err := resolver.Init(md)
	require.NoError(t, err)

	request := nr.ResolveRequest{ID: "testAppID", Port: 1234}
	pt, err := resolver.ResolveID(request)

	// assert
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:1234", pt)
}
