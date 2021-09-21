// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kubernetes

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/kit/logger"
)

func TestGetNamespace(t *testing.T) {
	t.Run("has namespace metadata", func(t *testing.T) {
		store := kubernetesSecretStore{logger: logger.NewLogger("test")}
		namespace := "a"

		ns, err := store.getNamespaceFromMetadata(map[string]string{"namespace": namespace})
		assert.Nil(t, err)
		assert.Equal(t, namespace, ns)
	})

	t.Run("has namespace env", func(t *testing.T) {
		store := kubernetesSecretStore{logger: logger.NewLogger("test")}
		os.Setenv("NAMESPACE", "b")

		ns, err := store.getNamespaceFromMetadata(map[string]string{})
		assert.Nil(t, err)
		assert.Equal(t, "b", ns)
	})

	t.Run("no namespace", func(t *testing.T) {
		store := kubernetesSecretStore{logger: logger.NewLogger("test")}
		os.Setenv("NAMESPACE", "")
		_, err := store.getNamespaceFromMetadata(map[string]string{})

		assert.NotNil(t, err)
		assert.Equal(t, "namespace is missing on metadata and NAMESPACE env variable", err.Error())
	})
}
