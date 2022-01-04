// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kubernetes

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func TestParseMetadata(t *testing.T) {
	nsName := "fooNamespace"
	t.Run("parse metadata", func(t *testing.T) {
		resyncPeriod := time.Second * 15
		m := bindings.Metadata{}
		m.Properties = map[string]string{"namespace": nsName, "resyncPeriodInSec": "15"}

		i := kubernetesInput{logger: logger.NewLogger("test")}
		i.parseMetadata(m)

		assert.Equal(t, nsName, i.namespace, "The namespaces should be the same.")
		assert.Equal(t, resyncPeriod, i.resyncPeriodInSec, "The resyncPeriod should be the same.")
	})
	t.Run("parse metadata no namespace", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"resyncPeriodInSec": "15"}

		i := kubernetesInput{logger: logger.NewLogger("test")}
		err := i.parseMetadata(m)

		assert.NotNil(t, err, "Expected err to be returned.")
		assert.Equal(t, "namespace is missing in metadata", err.Error(), "Error message not same.")
	})
	t.Run("parse metadata invalid resync period", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"namespace": nsName, "resyncPeriodInSec": "invalid"}

		i := kubernetesInput{logger: logger.NewLogger("test")}
		err := i.parseMetadata(m)

		assert.Nil(t, err, "Expected err to be nil.")
		assert.Equal(t, nsName, i.namespace, "The namespaces should be the same.")
		assert.Equal(t, time.Second*10, i.resyncPeriodInSec, "The resyncPeriod should be the same.")
	})
}
