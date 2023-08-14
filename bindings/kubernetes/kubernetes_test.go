/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

		assert.Equal(t, nsName, i.metadata.Namespace, "The namespaces should be the same.")
		assert.Equal(t, resyncPeriod, i.metadata.ResyncPeriod, "The resyncPeriod should be the same.")
	})
	t.Run("parse metadata no namespace", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"resyncPeriodInSec": "15"}

		i := kubernetesInput{logger: logger.NewLogger("test")}
		err := i.parseMetadata(m)

		assert.Error(t, err, "Expected err to be returned.")
		assert.ErrorContains(t, err, "namespace is missing in metadata", "Error message not same.")
	})
}
