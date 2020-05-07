// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------
package postgresql

import (
	"testing"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestInitConfiguration(t *testing.T) {

	tests := []struct {
		name        string
		props       map[string]string
		expectedErr string
	}{
		{
			name:        "Empty",
			props:       map[string]string{},
			expectedErr: errMissingConnectionString,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			postgresStore := NewPostgreSQLStateStore(logger.NewLogger("test"))

			metadata := state.Metadata{
				Properties: tt.props,
			}

			err := postgresStore.Init(metadata)
			assert.NotNil(t, err)

			if tt.expectedErr != "" {
				assert.Equal(t, err.Error(), tt.expectedErr)
			}
		})
	}
	
}